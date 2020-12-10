package oot.dht;

import oot.be.BEValue;
import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * Base class for all operations that run inside the local node,
 * includes simple protocol operation, complex operations on top of protocol messages,
 * possible utility/maintenance operations and operations, spawned on received messages
 *
 * Provides non blocking way to perform various tasks inside a node.
 */
abstract class Operation {

    // alias for internal usage, indicates operation must be finished
    static final boolean OPERATION_FINISH = true;

    // alias for internal usage, indicates operation must be proceeded with execution
    static final boolean OPERATION_RERUN = false;

    // max allowed number of errors for an operation to stop
    static final int MAX_SEND_ERRORS = 2;

    // default timeout for an operation to stop, in milliseconds
    static final long DEFAULT_OPERATION_TIMEOUT = 4_000L;

    // transaction id of the operation,
    // could be modified during several-step-operations,
    // need array as external transactions could send something long,
    // usually populated when operation is added for execution
    byte[] transaction;

    // start time of the operation to track timeouts
    long timestamp;
    // timeout could be different for various operations
    public long timeout;

    // ref to local node
    Node node;

    // counts numbers of datagrams sent,
    // need this in case if we can't send all datagrams in
    // one 'update' turn
    int sentCounter = 0;

    // counts number of received datagrams for this operation
    int recvCounter = 0;

    // tracks number of errors to end the operation
    int errCounter = 0;

    /**
     * generic constructor
     * @param _timestamp start time of the operation
     * @param _timeout timeout of the operation
     */
    Operation(Node _node, long _timestamp, long _timeout) {
        node = _node;
        timestamp = _timestamp;
        timeout = _timeout;
    }

    /**
     * default timings constructor,
     * uses current timestamp as start time and DEF_TIMEOUT as timeout
     */
    Operation(Node _node) {
        this(_node, System.currentTimeMillis(), DEFAULT_OPERATION_TIMEOUT);
    }

    /**
     * processes any response from the outer space,
     * it's a good idea to check external address for validness
     * with transaction id of this operation
     * @param address address of the sending party
     * @param response response received, only the internal response element
     * @return must return true if operation must be discarded after processing
     */
    boolean receive(InetSocketAddress address, BEValue response) {
        return OPERATION_RERUN;
    }

    /**
     * called to notify operation about error received in bounds of it's transaction,
     * it's a good idea to check address too
     * @param address address of the sending party
     * @param response response received, only the error element
     * @return must return true if operation must be discarded after processing
     */
    boolean error(InetSocketAddress address, BEValue response) {
        return OPERATION_RERUN;
    }

    /**
     * update operation's state, called periodically
     * to track timeouts and proceed with steps inside the operation,
     * each operation is allowed to exclusively use send buffer during this call
     * @return must return true if operation must be discarded after processing
     */
    boolean update() {
        return OPERATION_RERUN;
    }

    /**
     * service method, sends prepared send buffer with {@link Node#send(InetSocketAddress)}
     * and tracks local send/error counters
     * @param address destination address to send too
     * @return true if datagram was successfully sent and
     * false otherwise (that could be buffer overflow or some io error)
     */
    protected boolean send(InetSocketAddress address) {
        boolean result = node.send(address);
        if (result) {
            // ok, track successful messages
            sentCounter++;
        } else {
            // something happened, track number of errors
            errCounter++;
        }
        return result;
    }


    /**
     * sets id ot his transaction
     * @param id id to be set
     */
    void setTransaction(short id) {
        if ((transaction == null) || (transaction.length != 2)) {
            transaction = new byte[2];
        }
        transaction[0] = (byte)((id >> 8) & 0xFF);
        transaction[1] = (byte)(id & 0xFF);
    }

    /**
     * sets id of this transaction
     * @param id id to be set
     */
    void setTransaction(byte[] id) {
        transaction = id;
    }

    /**
     * tests if this operation's transaction id is the same as the specified one
     * @param id transaction id to check agains local one
     * @return true if identical
     */
    boolean cmpTransaction(byte[] id) {
        return Arrays.equals(transaction, id);
    }
}
