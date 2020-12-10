package oot.dht;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * base class for ping operations that check remote peers and/or finds their ids
 * in bounds of one operation
 */
abstract class PingOperation extends Operation {

    /**
     * template for outgoing messages,
     * 2 bytes transaction id and 20 bytes hash id must be substituted
     */
    static byte[] TEMPLATE = "d1:y1:q1:q4:ping1:t2:TX1:ad2:id20:XXXXXXXXXXXXXXXXXXXXee"
            .getBytes(StandardCharsets.UTF_8);

    /**
     * position of transaction id in the template
     */
    static final int TEMPLATE_POS_TX = 21;
    /**
     * position of hash id in the template
     */
    static final int TEMPLATE_POS_HASH = 34;


    /**
     * allowed constructor
     * @param _node ref to local node
     */
    public PingOperation(Node _node) {
        super(_node);
    }

    /**
     * populates the specified buffer with the message
     * review:
     * remove extra copy of the id part, #TEMPLATE could be updated
     * in constructor as node is must be stable
     * @param buffer buffer to populate
     * @param tx transaction id, must be 2 bytes array
     */
    void populateSendBuffer(ByteBuffer buffer, byte[] tx) {
        buffer.clear();
        buffer.put(TEMPLATE);
        buffer.put(TEMPLATE_POS_TX, tx[0]).put(TEMPLATE_POS_TX + 1, tx[1]);
        buffer.put(TEMPLATE_POS_HASH, node.id.getBytes());
        buffer.flip();
    }
}
