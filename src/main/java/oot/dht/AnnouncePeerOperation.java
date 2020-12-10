package oot.dht;

import oot.be.BEValue;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;

/**
 * runs announce_peer operation against the list of specific nodes,
 * sends multiple queries in bounds of one operation,
 *
 * nodes MUST be previously queried with get_peers to receive
 * active tokens for announcing, nodes without token will be skipped
 */
class AnnouncePeerOperation extends Operation {

    /**
     * template prefix of the message
     */
    private final static byte[] TEMPLATE = "d1:t2:TX1:y1:q1:q13:announce_peer1:ad"
            .getBytes(StandardCharsets.UTF_8);
    /**
     * position of transaction id in the template
     */
    private static final int TEMPLATE_POS_TX        = 6;

    /**
     * list of nodes to send announce too
     */
    private final List<RoutingTable.RemoteNode> nodes;
    /**
     * hash of the torrent we are announcing
     */
    private final HashId hash;
    /**
     * optional callback to be notified on finish
     */
    private final Consumer<Boolean> callback;
    /**
     * port of the linked peer used for peer-2-peer communications
     */
    private final int port;

    /**
     * allowed constructor
     * @param _node ref to local node
     * @param _nodes list of nodes to send queries too, could be modified - filtered out
     * @param _hash hash to announce
     * @param _port port of the linked peer for peer-2-peer communications
     * @param _callback callback to send results too
     */
    public AnnouncePeerOperation(
            Node _node,
            List<RoutingTable.RemoteNode> _nodes,
            HashId _hash,
            int _port,
            /* boolean implied */
            Consumer<Boolean> _callback)
    {
        super(_node);
        hash = _hash;
        callback = _callback;
        port = _port;

        nodes = _nodes;

        // remove nodes we don't have token to communicate with,
        // that must be done outside of this operation
        for (int i = nodes.size() - 1; 0 <= i; i--) {
            RoutingTable.RemoteNode rNode = nodes.get(i);
            if ((rNode.token == null) || (rNode.token.length == 0)) {
                nodes.remove(i);
            }
        }
    }

    /**
     * populates send buffer with operation message
     * review: remove extra copy of the id part
     * @param buffer buffer to populate
     * @param tx transaction id, only 2 low bytes will be used
     * @param token token to use
     */
    void populateSendBuffer(ByteBuffer buffer, byte[] tx, byte[] token) {
        buffer.clear();

        buffer.put(TEMPLATE);
        buffer.put(TEMPLATE_POS_TX, tx[0]).put(TEMPLATE_POS_TX + 1, tx[1]);

        buffer.put("2:id20".getBytes(StandardCharsets.UTF_8)).put(node.id.getBytes());
        buffer.put("9:info_hash20".getBytes(StandardCharsets.UTF_8)).put(hash.getBytes());

        buffer.put("5:token".getBytes(StandardCharsets.UTF_8));
        buffer.put(String.valueOf(token.length).getBytes(StandardCharsets.UTF_8));
        buffer.put((byte)':');
        buffer.put(token);

        buffer.put("4:porti".getBytes(StandardCharsets.UTF_8));
        buffer.put(String.valueOf(port).getBytes(StandardCharsets.UTF_8));
        buffer.put((byte)'e');

        buffer.put("12:implied_porti1e".getBytes(StandardCharsets.UTF_8));

        buffer.put((byte)'e');
        buffer.put((byte)'e');

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
                callback.accept(true);
            }
            // indicate operation is over
            return OPERATION_FINISH;
        }

        while (sentCounter < nodes.size())
        {
            RoutingTable.RemoteNode rNode = nodes.get(sentCounter);
            populateSendBuffer(node.sendBuffer, transaction, rNode.token);
            // prepare buffer for re-read
            node.sendBuffer.rewind();

            boolean isSent = send(rNode.address);
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
        //BEValue beQueried = response.dictionary.get("id");

        recvCounter++;

        // don't wait till check in update if we've received all responses
        if (recvCounter == nodes.size()) {
            if (callback != null) {
                callback.accept(true);
            }
            return OPERATION_FINISH;
        }

        return OPERATION_RERUN;
    }
}
