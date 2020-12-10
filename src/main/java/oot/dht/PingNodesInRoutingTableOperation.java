package oot.dht;

import oot.be.BEValue;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

/**
 * meta ping operation that checks nodes from the routing table,
 * operation always updates nodes in routing table with new status,
 * removes nodes if they don't respond.
 *
 * generates {@link PingPeersOperation} operations for each existing level
 * of the routing table
 *
 * uses the same transaction id for ping requests to different addresses
 * in bounds of one operation
 */
class PingNodesInRoutingTableOperation extends PingOperation {
    /**
     * default timeout for this operation,
     * as it performs many requests it usually takes more time
     */
    private final static long PING_ROUTING_NODES_TIMEOUT = 20_000L;

    /**
     * ref to the bucket of the routing table being processed
     */
    private RoutingTable.Bucket current;
    // debug: level or the routing being processed
    private int bucketLevel;

    /**
     * indicator to check if child operations are finished
     */
    boolean callbackCalled;

    /**
     * creates operation with the default timeout ({@link #PING_ROUTING_NODES_TIMEOUT})
     * @param _node ref to local node
     */
    public PingNodesInRoutingTableOperation(Node _node) {
        this(_node, PING_ROUTING_NODES_TIMEOUT);
    }

    /**
     * creates operation
     * @param _node ref to local node
     * @param _timeout specific timeout
     */
    public PingNodesInRoutingTableOperation(Node _node, long _timeout)
    {
        super(_node);

        current = node.routing.root;
        bucketLevel = 0;

        timeout = _timeout;

        // this starts the process
        callbackCalled = true;
    }

    @Override
    boolean update()
    {
        // check if there are no more levels in routing
        if (current == null) {
            return OPERATION_FINISH;
        }

        // global timeout protection
        if (timestamp + timeout < System.currentTimeMillis()) {
            return OPERATION_FINISH;
        }


        // wait while current level is being processed
        if (!callbackCalled) {
            return OPERATION_RERUN;
        }

        // get nodes that are ready to be refreshed
        List<InetSocketAddress> addresses = current.elements.stream()
                .filter(RoutingTable.RemoteNode::isReadyForPing)
                .map(rn -> rn.address)
                .collect(Collectors.toList());

        if (Node.DEBUG) {
            System.out.println("PingNodesInRoutingTable: level: " + bucketLevel + " , to ping: " + addresses.size());
        }

        if (addresses.isEmpty()) {
            current = current.child;
            bucketLevel++;
            return OPERATION_RERUN;
        }

        // reset processing flag and call operation
        callbackCalled = false;
        node.addOperation(new PingPeersOperation(node, addresses, true, ignored -> {
            callbackCalled = true;
            // remove possible dead nodes that were not updated
            current.removeDead();
            // switch to the next level
            current = current.child;
        }));

        return OPERATION_RERUN;
    }

    @Override
    boolean receive(InetSocketAddress address, BEValue response) {
        return OPERATION_RERUN;
    }
}
