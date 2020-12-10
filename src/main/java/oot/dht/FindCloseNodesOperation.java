package oot.dht;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Complex operation that runs iterative find_node operations
 * for the given hash, could be used for bootstrapping with
 * hash of the local node or to find nodes to announce a torrent,
 * calling party could be notified on operation finish via callback
 *
 */
class FindCloseNodesOperation extends Operation
{
    /**
     * internal states of the operation
     */
    private enum State {
        // initial state
        START,
        // searching nodes close to own id
        NODES
    }

    /**
     * max number of search iterations
     */
    private static final int DEFAULT_MAX_ITERATIONS = 32;
    /**
     * default timeout for the operation to stop, in milliseconds
     */
    static final long DEFAULT_OPERATION_TIMEOUT = 16_000L;

    // max iterations to perform
    private final int iterations;
    // target hash to search
    private final HashId target;
    // callback to be called on completion
    private final Consumer<Boolean> extCallback;
    // optional collection to start bootstrap from
    private final List<InetSocketAddress> seed;


    // tracks iterations while searching for nearest nodes -> nodes -> nodes -> ..
    private int iterationCounter;

    // tracks nodes we have sent request too
    private final Set<RoutingTable.RemoteNode> queried;

    // nodes received and to be queried with the next wave
    private List<RoutingTable.RemoteNode> nextNodes;

    // tracks completion of child operations
    private boolean callbackCalled;

    // current state of the operation
    private State state;


    /**
     * allowed constructor with full customization control,
     * operation will start from _seed collections of specified, otherwise
     * the closest nodes from the routing table will be used
     * @param _node ref to local node
     * @param _target hash to search for
     * @param _extCallback callback to be called on completion with true parameter
     *                     if routing has active nodes in it and false otherwise
     * @param _timeout operations' timeout
     * @param _iterations max allowed iterations
     * @param _seed optional initial collection to start search from (bootstrap mode)
     */
    public FindCloseNodesOperation(
            Node _node, HashId _target, Consumer<Boolean> _extCallback,
            long _timeout, int _iterations, List<InetSocketAddress> _seed)
    {
        super(_node, System.currentTimeMillis(), _timeout);

        target = _target;
        iterations = _iterations;
        extCallback = _extCallback;
        seed = _seed;

        queried = new HashSet<>();
        nextNodes = new ArrayList<>(RoutingTable.Bucket.K);

        state = State.START;
    }

    /**
     * allowed simplified constructor for frequent operations
     * @param _node ref to local node
     * @param _target target hash to seach for
     * @param _extCallback optional callback to notify
     */
    public FindCloseNodesOperation(
            Node _node, HashId _target, Consumer<Boolean> _extCallback)
    {
        this(_node, _target, _extCallback, DEFAULT_OPERATION_TIMEOUT, DEFAULT_MAX_ITERATIONS, null);
    }


    @Override
    boolean update()
    {
        // nodes that are being queried on the current iteration
        List<RoutingTable.RemoteNode> nodes;

        // global timeout protection
        if (timestamp + timeout < System.currentTimeMillis())
        {
            if (extCallback != null) {
                extCallback.accept(node.routing.hasAliveNodes());
            }
            return OPERATION_FINISH;
        }

        if (state == State.START)
        {
            if ((seed != null) && !seed.isEmpty()) {
                // use seed collection if specified, we don't have IDs
                // so just create temporary nodes
                HashId zero = HashId.zero();
                nodes = seed.stream()
                        .map(a -> node.routing.new RemoteNode(zero, a))
                        .collect(Collectors.toList());
            } else {
                // query routing table for closest nodes known,
                // at least one node is required
                nodes = node.routing.findClosestNodes(node.id);
            }

            if (nodes.isEmpty()) {
                // nowhere to search, finish the search
                if (extCallback != null) {
                    extCallback.accept(node.routing.hasAliveNodes());
                }
                return OPERATION_FINISH;
            }

            queried.addAll(nodes);

            // run search for the specified hash
            node.addOperation(new FindNodeOperation(node, nodes, target, rns -> {
                callbackCalled = true;
                nextNodes = rns;
            }));

            state = State.NODES;

            return OPERATION_RERUN;
        }

        if (state == State.NODES)
        {
            // wait for the result of the current wave
            if (!callbackCalled) {
                return OPERATION_RERUN;
            }
            callbackCalled = false;

            // increment number of waves
            iterationCounter++;
            if (Node.DEBUG) {
                System.out.println("FindCloseNodes: iteration: " + iterationCounter);
            }

            // nodes are independent and could return
            // nodes we have already queried, throw such nodes out
            nextNodes.removeAll(queried);

            // insert all new nodes into routing table
            for (RoutingTable.RemoteNode rNode: nextNodes) {
                node.routing.insert(rNode);
            }

            // have we reached the end of iterations?
            if (nextNodes.isEmpty() || (iterations < iterationCounter)) {
                // ok, let's use the last list of nodes
                if (Node.DEBUG) {
                    System.out.println("FindCloseNodes: finished, nodes in routing: " + node.routing.count());
                }
                if (extCallback != null) {
                    extCallback.accept(node.routing.hasAliveNodes());
                }
                return OPERATION_FINISH;
            }

            // track all queried (or errors)
            queried.addAll(nextNodes);

            // run next wave
            nodes = nextNodes;
            nextNodes = null;
            node.addOperation(new FindNodeOperation(node, nodes, target, rns -> {
                callbackCalled = true;
                nextNodes = rns;
            }));

            return OPERATION_RERUN;
        }

        return OPERATION_RERUN;
    }

}
