package oot.dht;

import oot.be.BEValue;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * base class for response operations, generated on queries received
 * from external nodes
 */
abstract class ResponseOperation extends Operation  {
    /**
     * address of the node that sent us the original query
     */
    protected InetSocketAddress address;
    /**
     * parsed query received
     */
    protected BEValue query;

    // transaction id extracted from the query
    protected byte[] tx;
    // requesting node id extracted
    protected byte[] id;
    // attributes dict extracted
    protected Map<String, BEValue> a;

    // indicates if query is valid or not,
    // used to stop the operation in the base class
    protected boolean queryCorrect = true;

    /**
     * allowed constructor
     * @param _node ref to local node
     * @param _timestamp start time of the operation
     * @param _timeout timeout for the operation
     * @param _address querying node address
     * @param _query query
     */
    protected ResponseOperation(
            Node _node,
            long _timestamp, long _timeout,
            InetSocketAddress _address,
            BEValue _query)
    {
        super(_node, _timestamp, _timeout);
        address = _address;
        query = _query;
    }

    /**
     * allowed constructor,
     * uses default timeout and current time
     * @param _node ref to local node
     * @param _address querying node address
     * @param _query query
     */
    protected ResponseOperation(Node _node,
                                InetSocketAddress _address,
                                BEValue _query)
    {
        this(_node, System.currentTimeMillis(), Operation.DEFAULT_OPERATION_TIMEOUT, _address, _query);
    }

    /**
     * checks and extracts common required parameters
     * @param txRequired is transaction id required
     * @param aRequired is attributes dict required
     * @param idRequired is requesting node is required
     * @return true if all required parameters present and false otherwise
     */
    protected boolean extract(boolean txRequired, boolean aRequired, boolean idRequired)
    {
        BEValue tmp;

        // extract transaction
        if (txRequired) {
            tmp = query.dictionary.get("t");
            if (BEValue.isBStringNotNull(tmp)) {
                tx = tmp.bString;
            } else {
                // incorrect query
                return false;
            }
        }

        // extract parameters
        tmp = query.dictionary.get("a");
        if (BEValue.isDictNotNull(tmp)) {
            a = tmp.dictionary;

            // extract node id
            if (idRequired) {
                tmp = a.get("id");
                if (BEValue.isBStringWithLength(tmp, HashId.HASH_LENGTH_BYTES)) {
                    id = tmp.bString;
                } else {
                    // incorrect query
                    return false;
                }
            }
        }
        else if (aRequired) {
            return false;
        }

        // all requested parameters present
        // and extracted
        return true;
    }

    /**
     * populates send buffer with operation's message,
     * must ne implemented in child classes with specific data
     * @param buffer buffer to populate
     */
    abstract void populateSendBuffer(ByteBuffer buffer);

    /**
     * adds binary encoded transaction id into the the buffer at current position,
     * this is used for custom transaction ids with size not equals to 2 bytes
     * @param buffer destination buffer
     * @param tx transaction id
     */
    protected void populateTransaction(ByteBuffer buffer, byte[] tx) {
        buffer.put(String.valueOf(tx.length).getBytes(StandardCharsets.UTF_8));
        buffer.put((byte)':');
        buffer.put(tx);
    }

    @Override
    boolean update()
    {
        if ((!queryCorrect)
                ||(Operation.MAX_SEND_ERRORS < errCounter)
                || (timestamp + timeout < System.currentTimeMillis()))
        {
            // can't send for some time
            // indicate operation is over
            return OPERATION_FINISH;
        }

        // let specific child class to populate it
        populateSendBuffer(node.sendBuffer);

        // prepare buffer for re-read,
        // this is more a protection if it wan't done in #populateSendBuffer
        node.sendBuffer.rewind();

        boolean isSent = send(address);
        if (!isSent) {
            // try to resend on the next update() turn
            return OPERATION_RERUN;
        }

        return OPERATION_FINISH;
    }
}
