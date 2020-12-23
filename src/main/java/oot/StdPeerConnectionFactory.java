package oot;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Selector;

/**
 * must bu refactored to generic factory
 */
class StdPeerConnectionFactory {

    /**
     * must be refactored in generic/std way
     */
    static class StdPeerConnectionBufferCache
    {
        static ByteBuffer getReceiveBuffer() {
            ByteBuffer buffer = ByteBuffer.allocateDirect(StdPeerConnectionFactory.RECV_BUFFER_SIZE);
            buffer.order(ByteOrder.BIG_ENDIAN);
            return buffer;
        }
        static ByteBuffer getSendBuffer() {
            ByteBuffer buffer = ByteBuffer.allocateDirect(StdPeerConnectionFactory.SEND_BUFFER_SIZE);
            buffer.order(ByteOrder.BIG_ENDIAN);
            return buffer;
        }

        void releaseReceiveBuffer(ByteBuffer buffer) {
        }
        void releaseSendBuffer(ByteBuffer buffer) {
        }
    }


    /**
     * max size of the data requested with PIECE message,
     * affects buffers (move to upper level?)
     */
    public static final int PIECE_BLOCK_MAX_SIZE = 16 << 10;
    /**
     * number of byte in PIECE message prefix (before data)
     * 4b length, 1b type, 2*4b params
     */
    public static final int MSG_PIECE_PREFIX_LENGTH = 13;

    /**
     * size of the receive buffer, it MUST be more than
     * size of the max allowed PIECE message plus some
     * operational space for small messages..
     * but seems it's best to size it to allow receive several
     * small messages and one PIECE inside first part (normal mode).
     * that could work well when pieces are mixed with small messages
     */
    public static final int RECV_BUFFER_SIZE = 256 + 2 * (PIECE_BLOCK_MAX_SIZE + MSG_PIECE_PREFIX_LENGTH);
    public static final int SEND_BUFFER_SIZE = 256 + 2 * (PIECE_BLOCK_MAX_SIZE + MSG_PIECE_PREFIX_LENGTH);

    /**
     * max allowed space of the receive buffer to be used in normal mode,
     * tail space must allow writing of max (PIECE - 1)
     */
    public static final int RECV_BUFFER_NORMAL_LIMIT = RECV_BUFFER_SIZE - (PIECE_BLOCK_MAX_SIZE + MSG_PIECE_PREFIX_LENGTH);
    public static final int SEND_BUFFER_NORMAL_LIMIT = SEND_BUFFER_SIZE - (PIECE_BLOCK_MAX_SIZE + MSG_PIECE_PREFIX_LENGTH);

    /**
     * amount of data in buffer to allow it's compaction (copy data to the beginning),
     * mostly to copy small parts of data
     * NOTE: it's a good idea to have it not less than BUFFER_SIZE % PIECE_MESSAGE_TOTAL_SIZE
     */
    public static final int SEND_BUFFER_COMPACT_LIMIT = 256;
    public static final int RECV_BUFFER_COMPACT_LIMIT = 256;

    StdPeerMessageCache pmCache = new StdPeerMessageCache();


    StdPeerConnection openConnection(Selector _selector, Torrent _torrent, Peer _peer)
    {
        ByteBuffer recvBuffer = StdPeerConnectionBufferCache.getReceiveBuffer();
        ByteBuffer sendBuffer = StdPeerConnectionBufferCache.getSendBuffer();
        return new StdPeerConnection(_selector, _torrent, _peer,
                recvBuffer, RECV_BUFFER_NORMAL_LIMIT, RECV_BUFFER_COMPACT_LIMIT,
                sendBuffer, SEND_BUFFER_NORMAL_LIMIT, SEND_BUFFER_COMPACT_LIMIT,
                pmCache);
    }

    void closeConnection(PeerConnection pc) {
        if (pc instanceof StdPeerConnection) {
            // release buffers
        }
    }
}
