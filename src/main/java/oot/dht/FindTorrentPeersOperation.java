package oot.dht;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Complex operation to searches for peers that stores
 * any specific torrent (hash)
 */
public class FindTorrentPeersOperation extends Operation
{
    /**
     * default timeout for operation to stop, in milliseconds
     */
    public static final long OPERATION_TIMEOUT = 64_000L;
    /**
     * max number of iteration during search process
     */
    public  static final int MAX_ITERATIONS = 100;
    /**
     * max number of closest nodes to use on each iteration
     */
    public  static final int MAX_NODES_IN_ITERATION = 16;

    /**
     * target hash to search
     */
    private final HashId target;

    // tracks nodes we have sent request too
    protected Set<RoutingTable.RemoteNode> queried;

    // all peers found
    protected Set<InetSocketAddress> peers;

    // nodes that are being queried on the current iteration
    //List<RoutingTable.RemoteNode> nodes;

    // nodes found on the current iteration
    protected List<RoutingTable.RemoteNode> nodesFound;
    // peers found on the current iteration
    protected List<InetSocketAddress> peersFound;

    // tracks iterations while searching for nearest nodes nodes -> nodes -> nodes -> ..
    protected int iterationCounter;

    // completion indicator for child operations
    protected boolean callbackCalled;

    // callback to be notified on finish
    protected Consumer<Set<InetSocketAddress>> extCallback;

    // service list of distinct nodes for the next iteration
    protected List<RoutingTable.RemoteNode> nodesDistinct;
    // distances to distinct nodes
    protected HashId[] nodesDistinctDistances;

    /**
     * internal states of the operation
     */
    protected enum State {
        // initial state
        START,
        // searching nodes close to own id
        NODES
    }

    // current state of the operation
    protected  State state;

    /**
     * allowed constructor
     * @param _node ref to local node
     * @param _target target hash to search for
     * @param _extCallback callback to call on finish
     */
    public FindTorrentPeersOperation(Node _node, HashId _target, Consumer<Set<InetSocketAddress>> _extCallback)
    {
        this(_node, _target, OPERATION_TIMEOUT, MAX_NODES_IN_ITERATION, _extCallback);
    }

    /**
     * allowed constructor
     * @param _node ref to local node
     * @param _target target hash to search for
     * @param _opTimeout custom timeout for operations
     * @param _maxNodesInIteration max number of new nodes to try in each iteration
     * @param _extCallback callback to call on finish
     */
    public FindTorrentPeersOperation(
            Node _node, HashId _target,
            long _opTimeout, int _maxNodesInIteration,
            Consumer<Set<InetSocketAddress>> _extCallback)
    {
        super(_node, System.currentTimeMillis(), _opTimeout);
        target = _target;
        extCallback = _extCallback;
        queried = new HashSet<>();
        peers = new HashSet<>();

        nodesDistinct = new ArrayList<>(_maxNodesInIteration);
        nodesDistinctDistances = new HashId[_maxNodesInIteration];

        state = State.START;
    }

    @Override
    protected boolean update()
    {
        // global timeout protection
        if (timestamp + timeout < System.currentTimeMillis())
        {
            if (extCallback != null) {
                extCallback.accept(peers);
            }
            return OPERATION_FINISH;
        }

        if (state == State.START)
        {
            // query routing table for closest nodes known
            List<RoutingTable.RemoteNode> nodes = node.routing.findClosestNodes(target);
            if (nodes.isEmpty()) {
                // nowhere to search, finish
                if (extCallback != null) {
                    extCallback.accept(peers);
                }
                return OPERATION_FINISH;
            }

            queried.addAll(nodes);

            // run initial search for target torrent
            node.addOperation(new GetPeersOperation(node, nodes, target, (n, p) -> {
                callbackCalled = true;
                nodesFound = n;
                peersFound = p;
            }));

            state = State.NODES;
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

            // update collection of all found peers
            // todo: notify every time?
            peers.addAll(peersFound);

            // NOTE
            // foundNodes are 'unlinked' from the routing table,
            // they are new objects not stores in the tree

            // queried nodes are independent and could return
            // nodes we have already queried, throw such nodes out
            nodesFound.removeAll(queried);

            // limit list of nodes as it could grow
            // very quickly: 8 nodes --> 64 --> 512 --> ...
            // remove duplicates and use only closest ones
            nodesDistinct.clear();
            for (RoutingTable.RemoteNode rNode: nodesFound) {
                node.routing.populateClosestList(target, nodesDistinct, nodesDistinctDistances, rNode);
            }

            // have we reached the end of iterations ?
            if (nodesDistinct.isEmpty() || (MAX_ITERATIONS < iterationCounter)) {
                if (extCallback != null) {
                    extCallback.accept(peers);
                }
                return OPERATION_FINISH;
            }

            // track all queried (or errors)
            queried.addAll(nodesDistinct);

            // run next wave, this will insert nodes into the routing
            node.addOperation(new GetPeersOperation(node, nodesDistinct, target, (n, p) -> {
                callbackCalled = true;
                nodesFound = n;
                peersFound = p;
            }));
            return OPERATION_RERUN;
        }

        return OPERATION_RERUN;
    }
}
