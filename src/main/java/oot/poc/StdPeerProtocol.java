package oot.poc;

import oot.dht.HashId;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;

/**
 * Peer to peer protocol utilities
 */
public class StdPeerProtocol {

    // debug switch
    private static final boolean DEBUG = false;

    /**
     * fixed identification of the protocol, fixed by the specification,
     * length is 19 bytes
     */
    public static final byte[] PROTOCOL_ID = "BitTorrent protocol".getBytes(StandardCharsets.UTF_8);

    /**
     * collection of known clients' abbreviations
     */
    private final static Map<String, String> CLIENTS_DASH = Map.ofEntries(
            Map.entry("AG", "Ares"),
            Map.entry("A~", "Ares"),
            Map.entry("AR", "Arctic"),
            Map.entry("AV", "Avicora"),
            Map.entry("AX", "BitPump"),
            Map.entry("AZ", "Azureus"),
            Map.entry("BB", "BitBuddy"),
            Map.entry("BC", "BitComet"),
            Map.entry("BF", "Bitflu"),
            Map.entry("BG", "BTG (uses Rasterbar libtorrent)"),
            Map.entry("BR", "BitRocket"),
            Map.entry("BS", "BTSlave"),
            Map.entry("BX", "~Bittorrent X"),
            Map.entry("CD", "Enhanced CTorrent"),
            Map.entry("CT", "CTorrent"),
            Map.entry("DE", "DelugeTorrent"),
            Map.entry("DP", "Propagate Data Client"),
            Map.entry("EB", "EBit"),
            Map.entry("ES", "electric sheep"),
            Map.entry("FT", "FoxTorrent"),
            Map.entry("FW", "FrostWire"),
            Map.entry("FX", "Freebox BitTorrent"),
            Map.entry("GS", "GSTorrent"),
            Map.entry("HL", "Halite"),
            Map.entry("HN", "Hydranode"),
            Map.entry("KG", "KGet"),
            Map.entry("KT", "KTorrent"),
            Map.entry("LH", "LH-ABC"),
            Map.entry("LP", "Lphant"),
            Map.entry("LT", "libtorrent"),
            Map.entry("lt", "libTorrent"),
            Map.entry("LW", "LimeWire"),
            Map.entry("MO", "MonoTorrent"),
            Map.entry("MP", "MooPolice"),
            Map.entry("MR", "Miro"),
            Map.entry("MT", "MoonlightTorrent"),
            Map.entry("NX", "Net Transport"),
            Map.entry("PD", "Pando"),
            Map.entry("qB", "qBittorrent"),
            Map.entry("QD", "QQDownload"),
            Map.entry("QT", "Qt 4 Torrent example"),
            Map.entry("RT", "Retriever"),
            Map.entry("S~", "Shareaza alpha/beta"),
            Map.entry("SB", "~Swiftbit"),
            Map.entry("SS", "SwarmScope"),
            Map.entry("ST", "SymTorrent"),
            Map.entry("st", "sharktorrent"),
            Map.entry("SZ", "Shareaza"),
            Map.entry("TN", "TorrentDotNET"),
            Map.entry("TR", "Transmission"),
            Map.entry("TS", "Torrentstorm"),
            Map.entry("TT", "TuoTu"),
            Map.entry("UL", "uLeecher!"),
            Map.entry("UT", "µTorrent"),
            Map.entry("UW", "µTorrent Web"),
            Map.entry("VG", "Vagaa"),
            Map.entry("WD", "WebTorrent Desktop"),
            Map.entry("WT", "BitLet"),
            Map.entry("WW", "WebTorrent"),
            Map.entry("WY", "FireTorrent"),
            Map.entry("XL", "Xunlei"),
            Map.entry("XT", "XanTorrent"),
            Map.entry("XX", "Xtorrent"),
            Map.entry("ZT", "ZipTorrent"),
            Map.entry("BD", "BD"),
            Map.entry("NP", "NP"),
            Map.entry("wF", "wF"));

    /**
     * processes fully received handshake message in the buffer, position is moved to "after message"
     * @param pc base peer we are working with
     * @param buffer buffer with data, position at the beginning of handshake
     * @return true on success parsing and false on possible errors (buffer will be in an unknown state)
     */
    static boolean processHandshake(StdPeerConnection pc, ByteBuffer buffer)
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

        if (DEBUG) System.out.println("RCVD: handshake");
        pc.onHandshake(reserved, HashId.wrap(hash), HashId.wrap(id));

        return true;
    }

    /**
     * processes fully received BITFIELD message in the buffer,
     * note: bitfield could contain only part of bits and be shorter
     * @param pc ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processBitfield(StdPeerConnection pc, ByteBuffer buffer, int length)
    {
        // extract number of pieces in torrent
        int pieces = (int) pc.torrent.metainfo.pieces;

        // seems (!?!) BITFIELD always must have the full length,
        // empty ending bits couldn't be omitted
        int bytes = (pieces + 7) >> 3;
        if (length != bytes) {
            if (DEBUG) System.out.println("BITFIELD message with incorrect length");
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
                    if (DEBUG) System.out.println("BITFIELD message contains incorrect bit set");
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
     * @param pc ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processPort(StdPeerConnection pc, ByteBuffer buffer, int length) {
        if (length != 2) {
            if (DEBUG) System.out.println("PORT message with incorrect length");
            return false;
        }

        int port = buffer.getShort();
        pc.onPort(port);

        return true;
    }

    /**
     * processed fully received HAVE message in the buffer
     * @param pc ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processHave(StdPeerConnection pc, ByteBuffer buffer, int length)
    {
        if (length != 4) {
            if (DEBUG) System.out.println("HAVE message with incorrect length");
            return false;
        }

        int index = buffer.getInt();
        pc.onHave(index);

        return true;
    }

    /**
     * processed fully received REQUEST message in the buffer
     * @param pc ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processRequest(StdPeerConnection pc, ByteBuffer buffer, int length) {
        if (length != 12) {
            if (DEBUG) System.out.println("REQUEST message with incorrect length");
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
     * @param pc ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processCancel(StdPeerConnection pc, ByteBuffer buffer, int length) {
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
     * @param pc ref to the receiving peer
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the data part of the message (without message type))
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processPiece(StdPeerConnection pc, ByteBuffer buffer, int length)
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
            //if (DEBUG) System.out.println("pc.onPiece() hasn't read all data from the buffer, recovering...");
            // recover correct position
            buffer.position(position + length - 8);
        }

        return true;
    }

    /**
     * processes fully received message stored in the buffer (buffer is guaranteed
     * to contain the full message), buffer's position is moved to "after message"
     * @param pc base peer we are working with
     * @param buffer buffer with data, position at the beginning of data part
     *               right after length and before the type elements [len 4 bytes | type 1 byte | data ]
     * @param length length of the message as read from the buffer
     * @return true on success parsing and false on possible errors (must be logged)
     */
    static boolean processMessage(StdPeerConnection pc, ByteBuffer buffer, int length)
    {
        if (length == 0) {
            // keep alive message are non-standard, without a type
            pc.onKeepAlive();
            return true;
        }

        // read type of the message
        byte type = buffer.get();
        if (DEBUG) System.out.println("RCVD   type:" + type + "    length: " + length);

        // decrease length to exclude message type
        length -= 1;

        switch (type) {
            case StdPeerMessage.CHOKE:
            case StdPeerMessage.UNCHOKE:
                if (length != 0) {
                    if (DEBUG) System.out.println("processMessage: choke/unchoke incorrect length");
                    return false;
                }
                pc.onChoke(type == StdPeerMessage.CHOKE);
                return true;

            case StdPeerMessage.INTERESTED:
            case StdPeerMessage.NOT_INTERESTED:
                if (length != 0) {
                    if (DEBUG) System.out.println("processMessage: interested/not_interested incorrect length");
                    return false;
                }
                pc.onInterested(type == StdPeerMessage.INTERESTED);
                return true;

            case StdPeerMessage.HAVE:
                return processHave(pc, buffer, length);

            case StdPeerMessage.BITFIELD:
                return processBitfield(pc, buffer, length);

            case StdPeerMessage.REQUEST:
                return processRequest(pc, buffer, length);

            case StdPeerMessage.PIECE:
                return processPiece(pc, buffer, length);

            case StdPeerMessage.CANCEL:
                return processCancel(pc, buffer, length);

            case StdPeerMessage.PORT:
                return processPort(pc, buffer, length);

            default:
                if (DEBUG) System.out.println("processMessage: unknown type: " + type);
                // skip this message
                buffer.position(buffer.position() + length - 1);
                return true;
        }
    }



    /**
     * populates buffer with the current message, that includes
     * length (4 bytes), type (1 byte) and data
     * @param pc ref to the base connection
     * @param buffer buffer to populate, remaining part MUST HAVE enough space
     * @param pm message to serialize into the buffer
     * @return true on success and false otherwise (impossible space checks, unknown types)
     */
    static boolean populate(StdPeerConnection pc, ByteBuffer buffer, StdPeerMessage pm)
    {
        switch (pm.type) {
            case StdPeerMessage.KEEPALIVE:
                if (buffer.remaining() < 4) {
                    return false;
                }
                buffer.putInt(0);
                if (DEBUG) System.out.println("SEND:   type: ka");
                return true;

            case StdPeerMessage.CHOKE:
            case StdPeerMessage.UNCHOKE:
            case StdPeerMessage.INTERESTED:
            case StdPeerMessage.NOT_INTERESTED:
                if (buffer.remaining() < 5) {
                    return false;
                }
                buffer.putInt(1);
                buffer.put(pm.type);
                if (DEBUG) System.out.println("SEND:   type:" + pm.type);
                return true;

            case StdPeerMessage.HAVE:
                if (buffer.remaining() < 9) {
                    return false;
                }
                buffer.putInt(5);
                buffer.put(pm.type);
                buffer.putInt(pm.index);
                if (DEBUG) System.out.println("SEND:   type: have    " + pm.index);
                return true;

            case StdPeerMessage.REQUEST:
            case StdPeerMessage.CANCEL:
                if (buffer.remaining() < 17) {
                    return false;
                }
                buffer.putInt(13);
                buffer.put(pm.type);
                buffer.putInt(pm.index);
                buffer.putInt(pm.begin);
                buffer.putInt(pm.length);
                if (DEBUG) System.out.println("SEND:   type:" + pm.type + "    " + pm.index + " " + pm.begin + " " + pm.length);
                return true;

            case StdPeerMessage.PORT:
                if (buffer.remaining() < 7) {
                    return false;
                }
                buffer.putInt(3);
                buffer.put(pm.type);
                buffer.putShort((short)pm.index);
                if (DEBUG) System.out.println("SEND:   type: port    " + pm.index);
                return true;

            case StdPeerMessage.BITFIELD:
                if (DEBUG) System.out.println("SEND:   type: bitfield");
                return populateBitfield(buffer, pm);

            case StdPeerMessage.PIECE:
                return populatePiece(buffer, pm);

            case StdPeerMessage.HANDSHAKE:
                if (DEBUG) System.out.println("SEND:   type: handshake");
                return populateHandshake(buffer, pm);

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
    private static boolean populateBitfield(ByteBuffer buffer, StdPeerMessage pm)
    {
        // number of bytes needed for all pieces
        int bytes = (pm.index + 7) >> 3;

        // this has minimal size to contain all set bits
        // in little ending format
        BitSet pieces = (BitSet) pm.params;
        byte[] data = pieces.toByteArray();

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
    private static boolean populatePiece(ByteBuffer buffer, StdPeerMessage pm) {

        if (buffer.remaining() < 13 + pm.block.remaining()) {
            return false;
        }
        buffer.putInt(9 + pm.block.remaining());
        buffer.put(pm.type);
        buffer.putInt(pm.index);
        buffer.putInt(pm.begin);
        buffer.put(pm.block);
        if (DEBUG) System.out.println("SEND:   type: piece    " + pm.index + " " + pm.begin + " " + pm.length);
        return true;
    }

    /**
     * populates buffer with handshake message
     * @param buffer buffer to populate, remaining part MUST HAVE enough space
     * @param pm message details
     * @return true on success and false otherwise (impossible space checks, unknown types)
     */
    static boolean populateHandshake(ByteBuffer buffer, StdPeerMessage pm)
    {
        HashId[] params = (HashId[]) pm.params;
        HashId infohash = params[0];
        HashId peerId = params[1];
            
        if (buffer.remaining() < 1 + 19 + 8 + 20 + 20) {
            return false;
        }
        // generate via ref to torrent or ???
        byte[] reserved = {0, 0, 0, 0, 0, 0, 0, 0};

        buffer.put((byte)19);
        buffer.put(PROTOCOL_ID);
        buffer.put(reserved);
        buffer.put(infohash.getBytes());
        buffer.put(peerId.getBytes());

        return true;
    }

    /**
     * extracts client name from hash id, supports not all the clients
     * @param id hash id of the client to decode
     * @return not null string name
     */
    static String extractClientNameFromId(HashId id)
    {
        if (id == null) {
            return "unknown";
        }

        byte[] data = id.getBytes();
        if (data[0] == 'M') {
            StringBuilder client = new StringBuilder("mainline ");
            int i = 1;
            while ((data[i] == '-') || Character.isDigit(data[i])) {
                client.append(data[i] & 0xFF);
            }
            return client.toString();
        }

        if ((data[0] == 'e') && (data[1] == 'x') && (data[2] == 'b') && (data[3] == 'c')) {
            return "BitComet " + (data[4] & 0xFF) + "." + (data[5] & 0xFF);
        }

        if ((data[0] == 'X') && (data[1] == 'B') && (data[2] == 'T')) {
            return "XBT " + (data[3] & 0xFF) + "." + (data[4] & 0xFF) + "." + (data[5] & 0xFF) + (data[6] == 'd' ? " debug" : "");
        }

        if ((data[0] == 'O') && (data[1] == 'P')) {
            return "Opera " + (data[2] & 0xFF) + "." + (data[3] & 0xFF) + "." + (data[4] & 0xFF) + "." + (data[5] & 0xFF);
        }

        if ((data[0] == '-') && (data[1] == 'M') && (data[2] == 'L')) {
            // -ML2.7.2-
            return "MLdonkey " + extractClientAsciiText(data, 3);
        }

        if ((data[0] == '-') && (data[1] == 'B') && (data[2] == 'O') && (data[3] == 'W')) {
            return "Bits on Wheels" + extractClientAsciiText(data, 4);
        }

        //if ((data[0] == 'Q')) {
        //    return "Queen Bee (?) " + decodePeerAsciiTail(data, 1);
        //}

        if ((data[0] == '-') && (data[1] == 'F') && (data[2] == 'G')) {
            return "FlashGet " + extractClientAsciiText(data, 3);
        }

        if (data[0] == 'A') {
            return "ABC " + extractClientAsciiText(data, 1);
        }
        if (data[0] == 'O') {
            return "Osprey Permaseed " + extractClientAsciiText(data, 1);
        }
        if (data[0] == 'Q') {
            return "BTQueue or Queen Bee " + extractClientAsciiText(data, 1);
        }
        if (data[0] == 'R') {
            return "Tribler " + extractClientAsciiText(data, 1);
        }
        if (data[0] == 'S') {
            return "Shadow " + extractClientAsciiText(data, 1);
        }
        if (data[0] == 'T') {
            return "BitTornado " + extractClientAsciiText(data, 1);
        }
        if (data[0] == 'U') {
            return "UPnP NAT Bit Torrent " + extractClientAsciiText(data, 1);
        }

        if ((data[0] == '-') && (data[7] == '-')) {
            String code = new String(data, 1, 2, StandardCharsets.UTF_8);
            StringBuilder client = new StringBuilder();
            String name = CLIENTS_DASH.get(code);
            if (name != null) {
                client.append(name);
            } else {
                client.append((char)data[1]).append((char)data[2]);
            }
            client.append(' ');
            client.append(Character.digit(data[3] & 0xFF, 10));
            client.append('.');
            client.append(Character.digit(data[4] & 0xFF, 10));
            client.append('.');
            client.append(Character.digit(data[5] & 0xFF, 10));
            client.append('.');
            client.append(Character.digit(data[6] & 0xFF, 10));
            return client.toString();
        }

        return "unknown " + extractClientAsciiText(data, 0);
    }

    /**
     * transforms part of byte data into string using
     * not terminated sequence of ascii letters and digits
     * @param data byte array to extract ascii like text
     * @param position start position in the array
     * @return not null string
     */
    static String extractClientAsciiText(byte[] data, int position)
    {
        StringBuilder tmp = new StringBuilder();
        while ((position < data.length)
                && (0 < data[position])
                && (('.' == data[position]) || ('-' == data[position]) || Character.isLetterOrDigit(data[position])))
        {
            tmp.append((char)data[position]);
        }
        return tmp.toString();
    }


}
