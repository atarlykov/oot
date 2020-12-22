package oot.poc;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Implements internal messages sent and received via peer 2 peer protocol
 */
public class StdPeerMessage {

    /**
     * known message types of the protocol
     */
    public static final byte CHOKE         = 0;
    public static final byte UNCHOKE       = 1;
    public static final byte INTERESTED    = 2;
    public static final byte NOT_INTERESTED    = 3;
    public static final byte HAVE          = 4;
    public static final byte BITFIELD      = 5;
    public static final byte REQUEST       = 6;
    public static final byte PIECE         = 7;
    public static final byte CANCEL        = 8;
    public static final byte PORT          = 9;

    /**
     * internal types for messages that don't have their type
     */
    static final byte KEEPALIVE    = -1;
    static final byte HANDSHAKE    = -2;


    /**
     * message type as specified above,
     * equals to the specification (except for internal ones)
     */
    byte type;

    /**
     * - 'index' field of REQUEST, CANCEL and PIECE messages,
     * - two low bytes are used for PORT message
     * - used as # of pieces for outgoing BITFIELD message
     */
    int index;
    /**
     * 'begin' field of REQUEST, CANCEL and PIECE messages
     */
    int begin;
    /**
     * 'length' field of REQUEST and CANCEL messages
     */
    int length;

    // enqueue time for outgoing requests,
    // used to track and remove active requests on timeout
    long timestamp;

    // ref to pieces' mask,
    // used only for bitfield messages
    //BitSet pieces;

    // ref to block of data, used with piece messages
    ByteBuffer block;

    // optional parameters, used only with some messages,
    // this consumes less memory than optional fields anyway,
    // cleared on return to the buffer
    Object params;

    /**
     * minimal allowed constructor
     * @param type type of the message
     */
    StdPeerMessage(byte type) {
        this.type = type;
    }

}
