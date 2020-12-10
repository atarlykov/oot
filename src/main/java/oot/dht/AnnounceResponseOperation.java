package oot.dht;

import oot.be.BEValue;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Operation for sending response to a received announce_peer query message
 */
class AnnounceResponseOperation extends ResponseOperation {

    /**
     * template for outgoing message,
     * [?] bytes transaction id and 20 bytes hash id must be substituted
     * note: usually transaction id is 2 bytes, but external side could send any size,
     * so why TX is at end of the template to allow extension
     */
    static byte[] TEMPLATE = "d1:y1:r1:rd2:id20:XXXXXXXXXXXXXXXXXXXXe1:t2:TXe"
            .getBytes(StandardCharsets.UTF_8);
    /**
     * position of hash id in the template
     */
    static final int TEMPLATE_POS_HASH = 18;
    /**
     * position of transaction element (len:data)
     */
    static final int TEMPLATE_POS_TX_ELEMENT = 42;

    /**
     * allowed constructor
     * @param _node ref to local node
     * @param _address remote address
     * @param _query ping query
     */
    public AnnounceResponseOperation(Node _node, InetSocketAddress _address, BEValue _query)
    {
        super(_node, _address, _query);

        byte[] target = null;
        byte[] token = null;
        int port = 0;
        boolean implied = false;

        // all parameters must present
        queryCorrect = extract(true, true, true);

        // extract specific parameters
        if (queryCorrect) {
            BEValue tmp = a.get("info_hash");
            if (BEValue.isBStringWithLength(tmp, HashId.HASH_LENGTH_BYTES)) {
                target = tmp.bString;
            } else {
                queryCorrect = false;
                return;
            }

            tmp = a.get("token");
            if (BEValue.isBStringNotEmpty(tmp)) {
                token = tmp.bString;
            } else {
                queryCorrect = false;
                return;
            }

            tmp = a.get("port");
            if (BEValue.isInteger(tmp)) {
                port = (int) tmp.integer;
            } else {
                queryCorrect = false;
                return;
            }

            tmp = a.get("implied_port");
            if (BEValue.isInteger(tmp) && (tmp.integer != 0)) {
                implied = true;
            }
        }

        if (!queryCorrect) {
            return;
        }


        // instance of remote node we are communicating with
        RoutingTable.RemoteNode rNode = node.tokens.get(HashId.wrap(id));
        if ((rNode == null)
                || !Arrays.equals(rNode.getTokenLocal(), token))
        {
            if (Node.DEBUG) {
                // it's possible for external node to remember different tokens for different torrents
                // must be checked
                if ((rNode != null) && !Arrays.equals(rNode.getTokenLocal(), token)) {
                    System.out.println(
                            address.toString() + "  external announce: invalid token (torrent dependent?)");
                }
            }

            queryCorrect = false;
            return;
        }

        rNode.updateLastActivityTime();

        HashId hashTarget = HashId.wrap(target);
        Set<RoutingTable.RemoteNode> downloaders = node.peers.computeIfAbsent(hashTarget, k -> new HashSet<>());
        downloaders.add(rNode);

        InetSocketAddress peerAddress = address;
        if (!implied) {
            rNode.setPeerPort(port);
            peerAddress = new InetSocketAddress(peerAddress.getAddress(), port);
        } else {
            rNode.setPeerPort(0);
        }

        // notify global listeners about announce received
        BiConsumer<HashId, InetSocketAddress> cbAnnounce = node.callbackAnnounce;
        if (cbAnnounce != null) {
            cbAnnounce.accept(hashTarget, peerAddress);
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
        buffer.put(TEMPLATE);
        buffer.put(TEMPLATE_POS_HASH, node.id.getBytes());

        if (tx.length == 2) {
            // most common case:
            // transaction id has standard length, insert into template
            buffer.put(TEMPLATE_POS_TX_ELEMENT + 2, tx);
        } else {
            // custom transaction id length,
            // generate full element
            buffer.position(TEMPLATE_POS_TX_ELEMENT);
            populateTransaction(buffer, tx);
            // end of enclosing dict
            buffer.put((byte)'e');
        }

        buffer.flip();
    }
}
