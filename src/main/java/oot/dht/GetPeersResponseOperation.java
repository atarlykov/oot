package oot.dht;

import oot.be.BEValue;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

/**
 * Operation for sending response to a received get_peers query message,
 * we always use the same token for the same external node (ip address)
 * and don't depend on id of the torrent requested
 */
class GetPeersResponseOperation extends ResponseOperation {

    /*
     * get_peers Query = {"t":"aa", "y":"q", "q":"get_peers", "a": {"id":"abcdefghij0123456789", "info_hash":"mnopqrstuvwxyz123456"}}
     *
     * Response with peers = {"t":"aa", "y":"r", "r": {"id":"abcdefghij0123456789", "token":"aoeusnth", "values": ["axje.u", "idhtnm"]}}
     * bencoded = d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re
     *
     * Response with closest nodes = {"t":"aa", "y":"r", "r": {"id":"abcdefghij0123456789", "token":"aoeusnth", "nodes": "def456..."}}
     * bencoded = d1:rd2:id20:abcdefghij01234567895:nodes9:def456...5:token8:aoeusnthe1:t2:aa1:y1:re
     *
     */

    /**
     * target info hash to look for
     */
    private byte[] target;
    /**
     * list of nodes from the routing table, closes to the target hash
     */
    private List<RoutingTable.RemoteNode> nodes;

    private Set<RoutingTable.RemoteNode> peers;

    /**
     * instance of remote node we are communicating with
     */
    private RoutingTable.RemoteNode rNode;

    /**
     * allowed constructor
     * @param _node ref to local node
     * @param _address remote address
     * @param _query ping query
     */
    public GetPeersResponseOperation(Node _node, InetSocketAddress _address, BEValue _query)
    {
        super(_node, _address, _query);
        // all parameters must present
        queryCorrect = extract(true, true, true);

        // extract specific parameters
        if (queryCorrect) {
            BEValue tmp = a.get("info_hash");
            if (BEValue.isBStringWithLength(tmp, HashId.HASH_LENGTH_BYTES)) {
                target = tmp.bString;
            } else {
                queryCorrect = false;
            }
        }

        if (!queryCorrect) {
            return;
        }

        // query is correct so
        // collect all data we'll need for sending the reply
        peers = node.peers.get(HashId.wrap(target));
        if ((peers == null) || peers.isEmpty())
        {
            // we don't know nodes announced themselves for this torrent,
            // let's return closest nodes
            nodes = node.routing.findClosestNodes(HashId.wrap(target));
            // just the protection as compact form designed only for ipv4
            for (int i = nodes.size() - 1; 0 <= i; i--) {
                if (!(nodes.get(i).address.getAddress() instanceof Inet4Address)) {
                    nodes.remove(i);
                }
            }
        } else {
            // just the protection as compact form designed only for ipv4
            peers.removeIf(rNode -> !(rNode.address.getAddress() instanceof Inet4Address));
        }

        HashId hid = HashId.wrap(id);

        // check if we've already communicated with this node
        // and have it in 'active' set
        rNode = node.tokens.get(hid);

        if (rNode == null) {
            // seems this is a fresh request, check
            // if have this node in routing
            rNode = node.routing.getRemoteNode(hid);
        }

        if (rNode == null) {
            // seems we know nothing about the node or
            // it was removed from all places (capacity limits?),
            // allocate new instance
            rNode = node.routing.new RemoteNode(hid, address);

            // this could fail due to capacity limitations,
            // but that's not a problem
            node.routing.insert(rNode);

            // finally store it as active
            node.tokens.put(hid, rNode);
        }

        rNode.updateLastActivityTime();
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
        assert node.id.getBytes().length == 20: "node id must be 20 bytes";
        buffer.put("1:rd2:id20:".getBytes(StandardCharsets.UTF_8));
        buffer.put(node.id.getBytes());

        // token
        byte[] tokenLocal = rNode.getTokenLocal();
        assert tokenLocal.length == 8: "generated token must be 8 bytes";
        buffer.put("5:token8:".getBytes(StandardCharsets.UTF_8));
        buffer.put(tokenLocal);

        if ((peers == null) || peers.isEmpty())
        {
            // peers are missing, generate closest nodes,
            // note: this is used even for empty list
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
        }
        else {
            // generate known peers
            buffer.put("6:valuesl".getBytes(StandardCharsets.UTF_8));
            for (RoutingTable.RemoteNode rn: peers) {
                buffer.put((byte)'6');
                buffer.put((byte)':');

                buffer.put(rn.address.getAddress().getAddress());
                int port = rn.address.getPort();
                buffer.put((byte)((port >> 8) & 0xFF));
                buffer.put((byte)(port & 0xFF));
            }
            buffer.put((byte)'e');
        }

        // dictionaries end
        buffer.put((byte)'e');
        buffer.put((byte)'e');

        buffer.flip();
    }
}
