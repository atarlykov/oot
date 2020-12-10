package oot.dht;

import oot.be.BEValue;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * runs find_node query against the list of specific nodes.
 * note: it's not the generic "find_node" that queries only one node!
 *
 * this operation is used only during bootstrap to iteratively find
 * closest nodes to the local one, so this operation usually spawned
 * by some other local operation
 *
 * operation returns not more than {@link RoutingTable.Bucket#K} closest elements
 * in the result list (via callback)
 *
 * arguments:  {"id" : "<querying nodes id>", "target" : "<id of target node>"}
 * response: {"id" : "<queried nodes id>", "nodes" : "<compact node info>"}
 */
class FindNodeOperation extends Operation {
    /**
     * template of find_node message with 3 parameters to be set: transaction id,
     * local node id and target hash id to find
     */
    private final static byte[] TEMPLATE =
            "d1:t2:TX1:y1:q1:q9:find_node1:ad2:id20:XXXXXXXXXXXXXXXXXXXX6:target20:XXXXXXXXXXXXXXXXXXXXee"
                    .getBytes(StandardCharsets.UTF_8);

    /**
     * position of transaction id in the template
     */
    private static final int TEMPLATE_POS_TX        = 6;
    /**
     * position of node id in the template
     */
    private static final int TEMPLATE_POS_ID        = 39;
    /**
     * position of target hash in the template
     */
    private static final int TEMPLATE_POS_TARGET    = 70;

    // target hash we are looking for
    HashId target;

    // nodes that will be queries
    List<RoutingTable.RemoteNode> nodes;

    // collection of the result nodes, merged from
    // response received from nodes
    List<RoutingTable.RemoteNode> result;

    // utility array with distances to nodes in response
    HashId[] resultDistances;

    // callback to be called on finish
    Consumer<List<RoutingTable.RemoteNode>> callback;

    /**
     * allowed constructor
     * @param _node ref to local node
     * @param _nodes list of nodes to send queries too
     * @param _target target id we are looking for
     * @param _callback callback to send results too
     */
    public FindNodeOperation(
            Node _node,
            List<RoutingTable.RemoteNode> _nodes,
            HashId _target,
            Consumer<List<RoutingTable.RemoteNode>> _callback)
    {
        super(_node);
        target = _target;
        callback = _callback;

        nodes = _nodes;

        result = new ArrayList<>(RoutingTable.Bucket.K);
        resultDistances = new HashId[RoutingTable.Bucket.K];
    }

    /**
     * populates send buffer with operation message
     * review: remove extra copy of the id part
     * @param buffer buffer to populate
     * @param tx transaction id, only 2 low bytes will be used
     * @param target target node id to use in message
     */
    void populateSendBuffer(ByteBuffer buffer, byte[] tx, HashId target) {
        buffer.clear();
        buffer.put(TEMPLATE);
        buffer.put(TEMPLATE_POS_TX, tx[0]).put(TEMPLATE_POS_TX + 1, tx[1]);
        buffer.put(TEMPLATE_POS_ID, node.id.getBytes());
        buffer.put(TEMPLATE_POS_TARGET, target.getBytes());
        buffer.flip();
    }

    @Override
    boolean update()
    {
        if ((recvCounter == nodes.size()) ||
                (Operation.MAX_SEND_ERRORS < errCounter) ||
                (timestamp + timeout < System.currentTimeMillis()))
        {
            if (callback != null) {
                callback.accept(result);
            }
            // indicate operation is over
            return OPERATION_FINISH;
        }

        // we use the same tx number, current node id is the same too
        populateSendBuffer(node.sendBuffer, transaction, target);
        while (sentCounter < nodes.size()) {
            // prepare buffer for re-read
            node.sendBuffer.rewind();

            boolean isSent = send(nodes.get(sentCounter).address);
            if (!isSent) {
                // indicate the operation needs a re-run
                return OPERATION_RERUN;
            }
        }

        // indicate operation is still alive
        return OPERATION_RERUN;
    }


    @Override
    boolean receive(InetSocketAddress address, BEValue response)
    {
        recvCounter++;

        BEValue beQueried = response.dictionary.get("id");
        BEValue beNodes = response.dictionary.get("nodes");

        // that is ok for external nodes to simply drop nodes element
        if (BEValue.isBString(beNodes) && (beNodes.bString != null)) {
            List<RoutingTable.RemoteNode> remoteNodes = node.parseNodesCompactForm(beNodes.bString);
            // build closest list
            for (RoutingTable.RemoteNode rn: remoteNodes) {
                node.routing.populateClosestList(target,  result, resultDistances, rn);
            }
        }

        // don't wait till check in update if we've received all responses
        if (recvCounter == nodes.size()) {
            if (callback != null) {
                callback.accept(result);
            }
            return OPERATION_FINISH;
        }

        return OPERATION_RERUN;
    }

    /**
     * simply increments receive counter to end operation
     * when all responses are received, this greatly helps
     * during debug with long timeouts
     * @param address address of the sending party
     * @param response response received, only the error element
     * @return true if operation must be finished
     */
    @Override
    boolean error(InetSocketAddress address, BEValue response) {
        recvCounter++;
        // don't wait till check in update if we've received all responses
        if (recvCounter == nodes.size()) {
            if (callback != null) {
                callback.accept(result);
            }
            return OPERATION_FINISH;
        }
        return OPERATION_RERUN;
    }
}
