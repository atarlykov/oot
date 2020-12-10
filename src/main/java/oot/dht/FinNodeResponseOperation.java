package oot.dht;

import oot.be.BEValue;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Operation for sending response to a received find_node query message
 */
class FinNodeResponseOperation extends ResponseOperation {

    /*
     * find_node Query = {"t":"aa", "y":"q", "q":"find_node", "a": {"id":"abcdefghij0123456789", "target":"mnopqrstuvwxyz123456"}}
     * bencoded = d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe
     *
     * Response = {"t":"aa", "y":"r", "r": {"id":"0123456789abcdefghij", "nodes": "def456..."}}
     * bencoded = d1:rd2:id20:0123456789abcdefghij5:nodes9:def456...e1:t2:aa1:y1:re
     *
     */

    /**
     * list of nodes from the routing table, closes to the targer hash
     */
    private List<RoutingTable.RemoteNode> nodes;

    /**
     * allowed constructor
     * @param _node ref to local node
     * @param _address remote address
     * @param _query ping query
     */
    public FinNodeResponseOperation(Node _node, InetSocketAddress _address, BEValue _query)
    {
        super(_node, _address, _query);
        // all parameters must present
        queryCorrect = extract(true, true, true);
        // extract specific parameters
        if (queryCorrect) {
            BEValue tmp = a.get("target");
            if (BEValue.isBStringWithLength(tmp, HashId.HASH_LENGTH_BYTES)) {
                byte[] target = tmp.bString;
                nodes = node.routing.findClosestNodes(HashId.wrap(target));
                // just the protection as compact form designed only for ipv4
                for (int i = nodes.size() - 1; 0 <= i; i--) {
                    if (!(nodes.get(i).address.getAddress() instanceof Inet4Address)) {
                        nodes.remove(i);
                    }
                }
            } else {
                queryCorrect = false;
            }
        }

    }

    /**
     * populates send buffer with operation message
     * review: remove extra copy of the id part
     * @param buffer buffer to populate
     */
    void populateSendBuffer(ByteBuffer buffer)
    {
        buffer.clear();

        buffer.put("d1:y1:r1:t2:".getBytes(StandardCharsets.UTF_8));
        if (tx.length == 2) {
            // most common case:
            // transaction id has standard length, insert into template
            buffer.put(tx);
        } else {
            // custom transaction id length,
            // rollback length prefix "2:"
            buffer.position(buffer.position() - 2);
            populateTransaction(buffer, tx);
        }

        // node id
        buffer.put("1:rd2:id20:".getBytes(StandardCharsets.UTF_8));
        buffer.put(node.id.getBytes());

        // nodes
        buffer.put("5:nodes".getBytes(StandardCharsets.UTF_8));
        buffer.put(String.valueOf(nodes.size() * 26).getBytes(StandardCharsets.UTF_8));
        buffer.put((byte)':');

        for (RoutingTable.RemoteNode rn: nodes) {
            buffer.put(rn.id.getBytes());
            buffer.put(rn.address.getAddress().getAddress());
            int port = rn.address.getPort();
            buffer.put((byte)((port >> 8) & 0xFF));
            buffer.put((byte)(port & 0xFF));
        }

        // dictionaries end
        buffer.put((byte)'e');
        buffer.put((byte)'e');

        buffer.flip();
    }

}
