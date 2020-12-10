package oot.dht;

import oot.be.BEValue;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * runs get_peers operation against the list of specific nodes.
 * this is an extension of the generic get_peers query that supports
 * querying of several nodes inside one operation
 *
 * NOTES:
 * - original nodes (to be queried) are updated with {@link RoutingTable.RemoteNode#token}
 *   received in reply messages from them
 * - original nodes are inserted into the routing table if correct response is received,
 *   this inserts them or updates them in the routing table
 * - nodes received in response messages are NOT inserted (or checked against) into the routing table
 *   as their status is unknown yet (at least for some of them)
 *
 * callback will be notified with collections that could contain
 * duplicate elements (nodes and peers), this will not happen
 * if only one node is queried (generic get_peers)
 */
class GetPeersOperation extends Operation {
    /**
     * message template with transaction id, local node id and target hash parameters
     */
    private final static byte[] TEMPLATE =
            "d1:t2:TX1:y1:q1:q9:get_peers1:ad2:id20:XXXXXXXXXXXXXXXXXXXX9:info_hash20:XXXXXXXXXXXXXXXXXXXXee".
            getBytes(StandardCharsets.UTF_8);

    /**
     * position of transaction id in the template
     */
    private static final int TEMPLATE_POS_TX = 6;
    /**
     * position of node id in the template
     */
    private static final int TEMPLATE_POS_ID = 39;
    /**
     * position of hash in the template
     */
    private static final int TEMPLATE_POS_HASH = 73;

    // target hash to query for
    HashId target;

    // list of peer found
    List<InetSocketAddress> foundPeers;
    // list of nodes that could be queried further
    List<RoutingTable.RemoteNode> foundNodes;

    // nodes to query for peers
    List<RoutingTable.RemoteNode> nodes;

    // call back to notify on finish
    BiConsumer<List<RoutingTable.RemoteNode>, List<InetSocketAddress>> callback;

    /**
     * allowed constructor
     * @param _node ref to local node
     * @param _nodes list of nodes to query
     * @param _target target hash to search peers by
     * @param _callback callback to notify on finish
     */
    public GetPeersOperation(
            Node _node,
            List<RoutingTable.RemoteNode> _nodes, HashId _target,
            BiConsumer<List<RoutingTable.RemoteNode>, List<InetSocketAddress>> _callback)
    {
        super(_node);
        target = _target;
        nodes = _nodes;
        callback = _callback;

        foundPeers = new ArrayList<>();
        foundNodes = new ArrayList<>();
    }

    /**
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
        buffer.put(TEMPLATE_POS_HASH, target.getBytes());
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
                callback.accept(foundNodes, foundPeers);
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
                return false;
            }
        }

        // check here to not wait till next run of the update()
        if (recvCounter == nodes.size()) {
            if (callback != null) {
                callback.accept(foundNodes, foundPeers);
            }
            // indicate operation is over
            return OPERATION_FINISH;
        }

        // indicate operation is still alive
        return OPERATION_RERUN;
    }


    @Override
    boolean receive(InetSocketAddress address, BEValue response)
    {
        /*
         * response could return peers or nodes to query further,
         * formats are the following (by the spec):
         *   response: {"id" : "<queried nodes id>", "token" :"<opaque write token>", "values" : ["<peer 1 info string>", "<peer 2 info string>"]}
         *         or: {"id" : "<queried nodes id>", "token" :"<opaque write token>", "nodes" : "<compact node info>"}
         *
         * in the real life there are replies that:
         * - have 'nodes' AND 'values' together
         * - have 'values' that is empty list (list with zero elements)
         * - have 'values' with a number of the same elements (even like 5 elements that are all the same)
         */

        // count nodes responded
        recvCounter++;

        BEValue beQueried = response.dictionary.get("id");
        BEValue beToken = response.dictionary.get("token");
        BEValue beNodes = response.dictionary.get("nodes");
        BEValue beValues = response.dictionary.get("values");

        if ((beQueried == null) || !HashId.validate(beQueried.bString)) {
            // id of the answered node must be valid,
            // skip this answer otherwise
            return OPERATION_RERUN;
        }

        long now = System.currentTimeMillis();

        // save active token for the answered node and
        // update state in the routing (case of unknown node)
        HashId queriedNodeId = HashId.wrap(beQueried.bString);
        for (int i = 0; i < nodes.size(); i++) {
            RoutingTable.RemoteNode rNode = nodes.get(i);
            if (rNode.id.equals(queriedNodeId)) {
                if (Node.DEBUG) {
                    if (!BEValue.isBString(beToken)) {
                        System.out.println(address.toString()
                                + " get_peers returned empty token");
                    }
                    if ((rNode.token != null) && !Arrays.equals(rNode.token, beToken.bString)) {
                        System.out.println(address.toString()
                                        + " get_peers returned another token (need support?)  time:" + (now - rNode.timeToken));
                    }
                }

                if (BEValue.isBStringNotEmpty(beToken))
                {
                    rNode.token = beToken.bString;
                    // we don't know if original node is from the routing or not,
                    // so try to update there
                    node.routing.update(rNode);
                }

                break;
            }
        }

        if ((beNodes != null) && (beNodes.bString != null)) {
            // queried node knows more close nodes...
            List<RoutingTable.RemoteNode> remoteNodes = node.parseNodesCompactForm(beNodes.bString);
            foundNodes.addAll(remoteNodes);
        }

        if ((beValues != null) && beValues.isListNotEmpty()) {
            // queried node knows direct peers with the target hash (torrent)
            for(BEValue bePeer: beValues.list) {
                InetSocketAddress isa = node.parsePeerCompactForm(bePeer.bString);
                if (isa != null) {
                    foundPeers.add(isa);
                }
            }
        }

        // end operation in place (could be done in update after timeout)
        if (nodes.size() <= recvCounter) {
            if (callback != null) {
                callback.accept(foundNodes, foundPeers);
            }
            return OPERATION_FINISH;
        } else {
            return OPERATION_RERUN;
        }
    }
}
