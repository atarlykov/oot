package oot;

import oot.dht.HashId;

import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * extends base peer connection to provide standard handshake support for child classes
 */
abstract class StdHandshakePeerConnection extends PeerConnection {

    /**
     * handshake message size
     * todo: move to STD protocol
     */
    public static final int MSG_HANDSHAKE_LENGTH = 20 + 8 + 20 + 20;

    /**
     * interface to be provided be the calling part that accepts
     * connections and must provide additional parameters to a connection
     * after initial phase (like handshake) is finished
     */
    interface TorrentProvider {
        /**
         * @param torrentId hash code of torrent's meta info
         * @return torrent if known
         */
        Torrent getTorrent(HashId torrentId);
    }

    /**
     * tracks if remote handshake message
     * has been received or not, need it to switch
     * to "normal" messages processing
     */
    boolean handshaked = false;
    /**
     * timestamp of the handshake event
     */
    long timeHandshaked;

    /**
     * service flag to track handshake process
     */
    protected boolean handshakeReceived = false;
    /**
     * service flag to track handshake process
     */
    protected boolean handshakeSent = false;

    /**
     * implementation of the interface to be used to get torrent data for accepted connections
     */
    protected TorrentProvider torrentProvider;


    public StdHandshakePeerConnection(
            Selector _selector, Torrent _torrent, Peer _peer,
            ByteBuffer _receiveBuffer,
            int _receiveBufferNormalLimit, int _receiveBufferCompactionLimit,
            ByteBuffer _sendBuffer,
            int _sendBufferNormalLimit, int _sendBufferCompactionLimit,
            TorrentProvider _torrentProvider)
    {
        super(_selector, _peer, _torrent,
                _receiveBuffer, _receiveBufferNormalLimit, _receiveBufferCompactionLimit,
                _sendBuffer, _sendBufferNormalLimit, _sendBufferCompactionLimit);
        torrentProvider = _torrentProvider;
    }

    public StdHandshakePeerConnection(
            Selector _selector, SocketChannel _channel,
            Peer _peer,
            ByteBuffer _receiveBuffer,
            int _receiveBufferNormalLimit, int _receiveBufferCompactionLimit,
            ByteBuffer _sendBuffer,
            int _sendBufferNormalLimit, int _sendBufferCompactionLimit,
            TorrentProvider _torrentProvider)
    {
        super(_selector, _channel, _peer,
                _receiveBuffer, _receiveBufferNormalLimit, _receiveBufferCompactionLimit,
                _sendBuffer, _sendBufferNormalLimit, _sendBufferCompactionLimit);
        torrentProvider = _torrentProvider;
    }


    protected abstract void handshakeCompleted();

    @Override
    protected boolean hasEnqueuedDate() {
        return !handshaked && ((outgoing && !handshakeSent) || (!outgoing && handshakeReceived && !handshakeSent));
    }

    @Override
    protected int processReceiveBuffer(ByteBuffer rb)
    {
        // we have buffer in the following state:
        // .position - start of the data in the buffer
        // .limit    - end of  --""--

        // handshake is the first message of the std protocol
        if (!handshaked) {
            // still waiting for handshake, check if we have enough data
            if (rb.remaining() < MSG_HANDSHAKE_LENGTH) {
                // wait for the next turn, we (must) have enough space
                // in the buffer for the rest of this handshake (config issue)
                return MSG_HANDSHAKE_LENGTH - rb.remaining();
            } else {
                // enough data, parse handshake
                boolean correct = StdPeerProtocol.processHandshake(this, rb);
                if (!correct) {
                    return -1;
                }
            }
        }

        // we have handled the handshake already
        return 0;
    }


    @Override
    protected boolean populateSendBuffer(ByteBuffer sb)
    {
        if (handshaked) {
            // already handshaked, skip
            return false;
        }

        // check if we are ready to send handshake basing on
        // outgoing/incoming state
        if ((outgoing && !handshakeSent) ||
                (!outgoing && handshakeReceived))
        {
            // (there is always space in send buffer on new connection)
            StdPeerMessage pm = new StdPeerMessage(StdPeerMessage.HANDSHAKE);
            HashId[] params = new HashId[2];
            params[0] = torrent.getTorrentId();
            params[1] = torrent.client.id;
            pm.params = params;

            boolean populated = StdPeerProtocol.populateHandshake(sb, pm);
            if (!populated) {
                // indicate we have more data
                return true;
            }

            // consider handshake successfully sent
            handshakeSent = true;

            if (!outgoing) {
                // incoming connection, out reply
                // finishes the handshake phase
                handshaked = true;
                handshakeCompleted();
            }
        }


        // we have no more data to send on this level
        return false;
    }

    /**
     * called after receiving of handshake message from the peer
     * @param torrentId torrent hash identifier
     * @param peerId peer identifier
     * @param reserved reserved bytes
     */
    void onHandshake(HashId torrentId, HashId peerId, byte[] reserved)
    {
        timeHandshaked = System.currentTimeMillis();

        if (outgoing) {
            // that's our connection and we have received
            // reply to our initial handshake, consider
            // the whole procedure completed
            handshaked = true;
            handshakeCompleted();
        } else {
            // incoming connection, we must send
            // our handshake back so the whole procedure is still in progress

            // now we know id of the torrent for this connection,
            // check if client has it
            torrent = torrentProvider.getTorrent(torrentId);
            if (torrent == null) {
                //if (DEBUG) System.out.println("unknown torrent requested: " + torrentId);
                close(Peer.CloseReason.NORMAL);
            }

            handshakeReceived = true;
        }

        peer.peerId = peerId;
        peer.reserved = reserved;
    }


}
