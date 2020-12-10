package oot.dht;

import oot.be.BEValue;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Operation for sending response to a received ping query message
 */
class PingResponseOperation extends ResponseOperation {

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
    public PingResponseOperation(Node _node, InetSocketAddress _address, BEValue _query)
    {
        super(_node, _address, _query);
        // all parameters must present
        queryCorrect = extract(true, true, true);
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
