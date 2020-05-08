package oot;

import oot.dht.HashId;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;

/**
 * Peer to peer protocol utilities
 */
public class PeerProtocol {

    /**
     * fixed identification of the protocol, fixed by the specification,
     * length is 19 bytes
     */
    public static final byte[] PROTOCOL_ID = "BitTorrent protocol".getBytes(StandardCharsets.UTF_8);


    /**
     * processes fully received handshake message in the buffer, position is moved to "after message"
     * @param torrent root torrent
     * @param pc base peer we are working with
     * @param buffer buffer with data, position at the beginning of handshake
     * @return true on success parsing and false on possible errors (buffer will be in an unknown state)
     */
    static boolean processHandshake(Torrent torrent, PeerConnection pc, ByteBuffer buffer)
    {
        // message length must be fixed
        byte len =  buffer.get();
        if (len != (byte)19) {
            return false;
        }

        // check protocol (spec)
        byte[] protocol = new byte[19];
        buffer.get(protocol);
        if (!Arrays.equals(PROTOCOL_ID, protocol)) {
            return false;
        }

        byte[] reserved = new byte[8];
        byte[] hash = new byte[20];
        byte[] id = new byte[20];
        buffer.get(reserved);
        buffer.get(hash);
        buffer.get(id);

        pc.onHandshake(reserved, HashId.wrap(hash), HashId.wrap(id));

        return true;
    }

    /**
     * processed fully received BITFIELD message in the buffer,
     * note: bitfield could contain only part of bits and be shorter
     * @param torrent ref to the torrent
     * @param pc ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processBitfield(Torrent torrent, PeerConnection pc, ByteBuffer buffer, int length)
    {
        // extract number of pieces in torrent
        int pieces = (int) torrent.metainfo.pieces;

        // seems (!?!) BITFIELD always must have the full length,
        // empty ending bits couldn't be omitted
        int bytes = (pieces + 7) >> 3;
        if (length != bytes) {
            assert false: "BITFIELD message with incorrect length";
            return false;
        }

        BitSet mask = new BitSet(pieces);

        int index = 0;
        int bitIndex = 0;
        // read all the bytes in the message
        while (index < length)
        {
            int data = Byte.toUnsignedInt(buffer.get());

            for (int i = 7; 0 <= i; i--) {
                boolean isBitSet = ((data >> i) & 0x01) != 0;
                if (bitIndex < pieces) {
                    mask.set(bitIndex++, isBitSet);
                }
                else if (isBitSet) {
                    assert false: "BITFIELD message contains incorrect bit set";
                    // this could be used as the reason to drop the connection,
                    // ignore for now
                    // return false;
                }
            }
            index++;
        }

        pc.onBitField(mask);

        return true;
    }

    /**
     * processed fully received PORT message in the buffer
     * @param torrent ref to the torrent
     * @param pc ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processPort(Torrent torrent, PeerConnection pc, ByteBuffer buffer, int length) {
        if (length != 2) {
            assert false: "PORT message with incorrect length";
            return false;
        }

        int port = buffer.getShort();
        pc.onPort(port);

        return true;
    }

    /**
     * processed fully received HAVE message in the buffer
     * @param torrent ref to the torrent
     * @param pc ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processHave(Torrent torrent, PeerConnection pc, ByteBuffer buffer, int length) {
        if (length != 4) {
            assert false: "HAVE message with incorrect length";
            return false;
        }

        int index = buffer.getInt();
        pc.onHave(index);

        return true;
    }

    /**
     * processed fully received REQUEST message in the buffer
     * @param torrent ref to the torrent
     * @param pc ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processRequest(Torrent torrent, PeerConnection pc, ByteBuffer buffer, int length) {
        if (length != 12) {
            assert false: "REQUEST message with incorrect length";
            return false;
        }
        int index = buffer.getInt();
        int begin = buffer.getInt();
        int len = buffer.getInt();
        pc.onRequest(index, begin, len);

        return true;
    }

    /**
     * processed fully received CANCEL message in the buffer
     * @param torrent ref to the torrent
     * @param pc ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processCancel(Torrent torrent, PeerConnection pc, ByteBuffer buffer, int length) {
        if (length != 12) {
            return false;
        }
        // parameters will be validated on upper level
        int index = buffer.getInt();
        int begin = buffer.getInt();
        int len = buffer.getInt();
        pc.onCancel(index, begin, len);

        return true;
    }

    /**
     * processed fully received PIECE message in the buffer
     * @param torrent ref to the torrent
     * @param pc ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processPiece(Torrent torrent, PeerConnection pc, ByteBuffer buffer, int length)
    {
        if (length < 8) {
            return false;
        }
        int index = buffer.getInt();
        int begin = buffer.getInt();

        int position = buffer.position();
        pc.onPiece(buffer, index, begin, length - 8);

        // check if notified party has correctly read the data
        if ((position + length - 8) != buffer.position()) {
            assert false: "pc.onPiece() hasn't read all data from the buffer";
            // recover correct position
            buffer.position(position + length - 8);
        }

        return true;
    }

    /**
     * processes fully received message stored in the buffer (buffer is guaranteed
     * to contain the full message), buffer's position is moved to "after message"
     * @param torrent root torrent
     * @param pc base peer we are working with
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and before the type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the message as read from the buffer
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processMessage(Torrent torrent, PeerConnection pc, ByteBuffer buffer, int length) {

        if (length == 0) {
            // keep alive message are non-standard, without a type
            pc.onKeepAlive();
            return true;
        }

        // read type of the message
        byte type = buffer.get();

        //System.out.println("RCVD   type:" + type + "    length: " + length);

        // decrease length to exclude message type
        length -= 1;

        switch (type) {
            case PeerMessage.CHOKE:
            case PeerMessage.UNCHOKE:
                if (length != 0) {
                    assert false: "processMessage: choke/unchoke incorrect length";
                    return false;
                }
                pc.onChoke(type == PeerMessage.CHOKE);
                return true;

            case PeerMessage.INTERESTED:
            case PeerMessage.NOT_INTERESTED:
                if (length != 0) {
                    assert false: "processMessage: interested/not_interested incorrect length";
                    return false;
                }
                pc.onInterested(type == PeerMessage.INTERESTED);
                return true;

            case PeerMessage.HAVE:
                return processHave(torrent, pc, buffer, length);

            case PeerMessage.BITFIELD:
                return processBitfield(torrent, pc, buffer, length);

            case PeerMessage.REQUEST:
                return processRequest(torrent, pc, buffer, length);

            case PeerMessage.PIECE:
                return processPiece(torrent, pc, buffer, length);

            case PeerMessage.CANCEL:
                return processCancel(torrent, pc, buffer, length);

            case PeerMessage.PORT:
                return processPort(torrent, pc, buffer, length);

            default:
                assert false: "processMessage: unknown type: " + type;
                // skip this message
                buffer.position(buffer.position() + length - 1);
                return true;
        }
    }



    /**
     * populates buffer with the current message, that includes
     * length (4 bytes), type (1 byte) and data
     * @param buffer buffer to populate, remaining part MUST HAVE enough space
     * @param pm message to serialize into the buffer
     * @return true on success and false otherwise (impossible space checks, unknown types)
     */
    static boolean populate(ByteBuffer buffer, PeerMessage pm) {

        switch (pm.type) {
            case PeerMessage.KEEPALIVE:
                if (buffer.remaining() < 4) {
                    return false;
                }
                buffer.putInt(0);
                return true;

            case PeerMessage.CHOKE:
            case PeerMessage.UNCHOKE:
            case PeerMessage.INTERESTED:
            case PeerMessage.NOT_INTERESTED:
                if (buffer.remaining() < 5) {
                    return false;
                }
                buffer.putInt(1);
                buffer.put(pm.type);
                return true;

            case PeerMessage.HAVE:
                if (buffer.remaining() < 9) {
                    return false;
                }
                buffer.putInt(5);
                buffer.put(pm.type);
                buffer.putInt(pm.index);
                return true;

            case PeerMessage.REQUEST:
            case PeerMessage.CANCEL:
                if (buffer.remaining() < 17) {
                    return false;
                }
                buffer.putInt(13);
                buffer.put(pm.type);
                buffer.putInt(pm.index);
                buffer.putInt(pm.begin);
                buffer.putInt(pm.length);
                return true;

            case PeerMessage.PORT:
                if (buffer.remaining() < 7) {
                    return false;
                }
                buffer.putInt(3);
                buffer.put(pm.type);
                buffer.putShort((short)pm.index);
                return true;

            case PeerMessage.BITFIELD:
                return populateBitfield(buffer, pm);

            case PeerMessage.PIECE:
                return populatePiece(buffer, pm);

            case PeerMessage.HANDSHAKE:
                // this is NOT intended to be called via message interface,
                // populated directly via call to {@link PeerMessage#populatHandshake}
                return false;

            default:
                System.out.println("unknown type: " + pm.type);
                return false;
        }
    }

    /**
     * populates buffer with BITFIELD message
     * @param buffer buffer to populate, remaining part MUST HAVE enough space
     * @param pm message to serialize into the buffer
     * @return true on success and false otherwise (impossible space checks, unknown types)
     */
    private static boolean populateBitfield(ByteBuffer buffer, PeerMessage pm)
    {
        // number of bytes needed for all pieces
        int bytes = (pm.index + 7) >> 3;

        // this has minimal size to contain all set bits
        // in little ending format
        byte[] data = pm.pieces.toByteArray();

        if (buffer.remaining() < 5 + bytes) {
            return false;
        }

        buffer.putInt(1 + bytes);
        buffer.put(pm.type);

        // protection against too long data
        int limit = Math.min(data.length, bytes);
        for (int i = 0; i < limit; i++) {
            int value = data[i];
            // reverse bits and bytes and take top most byte
            value = Integer.reverse(value) >> 24;
            buffer.put((byte)value);
        }

        // tail zeros
        for (int i = 0; i < bytes - limit; i++) {
            buffer.put((byte)0);
        }
        return true;
    }

    /**
     * populates buffer with PIECE message
     * @param buffer buffer to populate, remaining part MUST HAVE enough space
     * @param pm message to serialize into the buffer
     * @return true on success and false otherwise (impossible space checks, unknown types)
     */
    private static boolean populatePiece(ByteBuffer buffer, PeerMessage pm) {

        if (buffer.remaining() < 13 + pm.block.remaining()) {
            return false;
        }
        buffer.putInt(9 + pm.block.remaining());
        buffer.put(pm.type);
        buffer.putInt(pm.index);
        buffer.putInt(pm.begin);
        buffer.put(pm.block);
        return true;
    }

    /**
     * populates buffer with handshake message
     * @param buffer buffer to populate, remaining part MUST HAVE enough space
     * @param infohash hash of the parent torrent
     * @param peerId unique and fixed local peer id
     * @return true on success and false otherwise (impossible space checks, unknown types)
     */
    static boolean populateHandshake(ByteBuffer buffer, HashId infohash, HashId peerId) {
        // generate via ref to torrent or ???
        byte[] reserved = {0, 0, 0, 0, 0, 0, 0, 0};

        buffer.put((byte)19);
        buffer.put(PROTOCOL_ID);
        buffer.put(reserved);
        buffer.put(infohash.getBytes());
        buffer.put(peerId.getBytes());

        return true;
    }

}
