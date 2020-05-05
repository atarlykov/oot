package oot;

import oot.dht.HashId;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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
     * @param peer base peer we are working with
     * @param buffer buffer with data, position at the beginning of handshake
     * @return true on success parsing and false on possible errors (buffer will be in an unknown state)
     */
    static boolean processHandshake(Torrent torrent, PeerConnection peer, ByteBuffer buffer) {
        byte len =  buffer.get();
        if (len != (byte)19) {
            return false;
        }
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

        peer.onHandshake(reserved, HashId.wrap(hash), HashId.wrap(id));

        return true;
    }

    /**
     * processed fully received BITFIELD message in the buffer,
     * note: bitfield could contain only part of bits and be shorter
     * @param torrent ref to the torrent
     * @param peer ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processBitfield(Torrent torrent, PeerConnection peer, ByteBuffer buffer, int length) {
        int index = 0;
        int bitIndex = 0;
        int bitIndexInByte = -1;


        // todo: separate access to pc internals, pc.onBit/onHave/onXXX()

        long pieces = torrent.metainfo.pieces;

        // read all the bytes in the message
        while (index < length) {
            int data = Byte.toUnsignedInt(buffer.get());

            for (int i = 7; 0 <= i; i--) {
                boolean isBitSet = ((data >> i) & 0x01) != 0;
                if (bitIndex < pieces) {
                    // protection against extra bytes/bits
                    peer.peerPieces.set(bitIndex++, isBitSet);
                }
                else if (isBitSet) {
                    System.out.println("incorrect bit in bitfield");
                }
            }
            index++;
        }

        peer.bitfieldReceived = true;

        return true;
    }

    /**
     * processed fully received PORT message in the buffer
     * @param torrent ref to the torrent
     * @param peer ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processPort(Torrent torrent, PeerConnection peer, ByteBuffer buffer, int length) {
        if (length != 2) {
            System.out.println("port length incorrect");
            return false;
        }
        int port = buffer.getShort();
        peer.onPort(port);
        return true;
    }

    /**
     * processed fully received HAVE message in the buffer
     * @param torrent ref to the torrent
     * @param peer ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processHave(Torrent torrent, PeerConnection peer, ByteBuffer buffer, int length) {
        if (length != 4) {
            System.out.println("have length incorrect");
            return false;
        }
        int index = buffer.getInt();
        // todo: validate index & call pc.onHave()
        peer.peerPieces.set(index);
        return true;
    }

    /**
     * processed fully received REQUEST message in the buffer
     * @param torrent ref to the torrent
     * @param peer ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processRequest(Torrent torrent, PeerConnection peer, ByteBuffer buffer, int length) {
        if (length != 12) {
            System.out.println("request length incorrect");
            return false;
        }
        int index = buffer.getInt();
        int begin = buffer.getInt();
        int len = buffer.getInt();
        peer.onRequest(index, begin, len);
        return true;
    }

    /**
     * processed fully received CANCEL message in the buffer
     * @param torrent ref to the torrent
     * @param peer ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processCancel(Torrent torrent, PeerConnection peer, ByteBuffer buffer, int length) {
        if (length != 12) {
            System.out.println("cancel length incorrect");
            return false;
        }
        // todo: validate data
        int index = buffer.getInt();
        int begin = buffer.getInt();
        int len = buffer.getInt();
        peer.onCancel(index, begin, len);
        return true;
    }

    /**
     * processed fully received PIECE message in the buffer
     * @param torrent ref to the torrent
     * @param peer ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processPiece(Torrent torrent, PeerConnection peer, ByteBuffer buffer, int length) {
        if (length < 8) {
            System.out.println("piece length incorrect");
            return false;
        }
        // todo: validate data
        int index = buffer.getInt();
        int begin = buffer.getInt();

        //System.out.println("piece: " + index + "  " + begin + "  " + (length - 8) + "   b:" + (begin >> 14));

        int position = buffer.position();
        peer.onPiece(buffer, index, begin, length - 8);

        // check if notified party has correctly read the data
        if ((position + length - 8) != buffer.limit()) {
            System.out.println("onPiece() hasn't read all the data from the buffer");
            // recover correct position
            buffer.position(position + length - 8);
        }

        return true;
    }

    /**
     * processes fully received message stored in the buffer (buffer is guaranteed
     * to contain the full message), buffer's position is moved to "after message"
     * @param torrent root torrent
     * @param peer base peer we are working with
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and before the type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the message as read from the buffer
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processMessage(Torrent torrent, PeerConnection peer, ByteBuffer buffer, int length) {

        if (length == 0) {
            // keep alive message are non-standard, without a type
            peer.onKeepAlive();
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
                    System.out.println("choke/unchoke incorrect length");
                    return false;
                }
                peer.onChoke(type == PeerMessage.CHOKE);
                return true;

            case PeerMessage.INTERESTED:
            case PeerMessage.NOT_INTERESTED:
                if (length != 0) {
                    System.out.println("interested/not_interested incorrect length");
                    return false;
                }
                peer.onInterested(type == PeerMessage.INTERESTED);
                return true;

            case PeerMessage.HAVE:
                return processHave(torrent, peer, buffer, length);

            case PeerMessage.BITFIELD:
                return processBitfield(torrent, peer, buffer, length);

            case PeerMessage.REQUEST:
                return processRequest(torrent, peer, buffer, length);

            case PeerMessage.PIECE:
                return processPiece(torrent, peer, buffer, length);

            case PeerMessage.CANCEL:
                return processCancel(torrent, peer, buffer, length);

            case PeerMessage.PORT:
                return processPort(torrent, peer, buffer, length);

            default:
                System.out.println("unknown type: " + type);
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
    private static boolean populateBitfield(ByteBuffer buffer, PeerMessage pm) {
        // this has minimal size to contain all set bits
        // in little ending format
        byte[] data = pm.pieces.toByteArray();

        if (buffer.remaining() < 5 + data.length) {
            return false;
        }

        buffer.putInt(1 + data.length);
        buffer.put(pm.type);

        for (int i = 0; i < data.length; i++) {
            int value = data[i];
            // reverse bits and bytes and take top most byte
            value = Integer.reverse(value) >> 24;
            buffer.put((byte)value);
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
