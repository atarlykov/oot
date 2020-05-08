package oot;

import oot.dht.HashId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Stream;

/**
 * handles connected peer with data for a specific torrent.
 * as we need a separate connection for each infohash,
 * this is unique object for each [remote peer, torrent]
 *
 * all operations are performed in the same thread, dedicated
 * by the Client to serve the Torrent
 */
public class PeerConnection {
    /**
     * max number of active piece requests (for blocks) could be sent
     * to any external peer (it's 5 by the spec, but should be higher on good connections)
     */
    public static final int DOWNLOAD_QUEUED_REQUESTS_MAX = 11;
    /**
     * timeout for outgoing request to wait for answer,
     * will be dropped and resent on timeout (to the same or
     * any other peer), milliseconds
     */
    public static final int DOWNLOAD_QUEUE_REQUESTS_TIMEOUT = 1000;
    /**
     * length of time period to aggregate downloaded and uploaded bytes,
     * in milliseconds
     */
    public static final int STATISTICS_PERIOD_LENGTH = 1000;
    /**
     * number of periods to store for download/upload statistics
     */
    public static final int STATISTICS_PERIODS = 8;

    /*
     * main containers:
     * - sendQueue          - queued messages to be sent to external peer
     * - blockRequests      - active block requests from our side (has been serialized into send buffer)
     * - peerBlockRequests  - active block requests from ext peer (received and being processed)
     *
     * 1. messages are enqueued into sendQueue, dependent messages are removed (CHOKE, CANCEL)
     * 2. sendQueue --> sendBuffer, here blockRequests and peerBlockRequests are tracked
     * 3. onReceive here too blockRequests and peerBlockRequests are tracked
     *
     * enqueue: [handles only queue logic]
     *   REQUEST   -> queue
     *   PIECE     -> queue
     *
     *   CHOKE     -> remove replies from queue, release block + all pm
     *   CANCEL    -> remove linked request from queue, release pm
     *
     *   statuses are updated right here
     *
     * send (serialize): [handles only bRs]
     *   ALWAYS       get/remove from queue + ...
     *
     *   REQUEST   -> [add to bRq], don't release pm
     *   PIECE     -> [del from pbRq], release block, release pm [can skip send if missing in bRs, choked]
     *
     *   CHOKE     -> can remove from pbRq, release all pm
     *   CANCEL    -> [del from bRq], release all pm
     *   OTHERS    -> release pm
     *
     * receive:
     *   onPiece   -> [del from bRq]
     *   onRequest -> [add to pbRq]
     *   onChoke   -> remove from queue + bRq, release all pm
     *   onCancel  -> remove from queue + pbRq, release block + all pm
     *
     *
     */

    /**
     * max size of the data requested with PIECE message,
     * affects buffers
     */
    public static final int PIECE_BLOCK_MAX_SIZE = 16 << 10;
    /**
     * number of byte in PIECE message prefix (before data)
     * 4b length, 1b type, 2*4b params
     */
    public static final int MSG_PIECE_PREFIX_LENGTH = 13;

    /**
     * handshake message size
     */
    public static final int MSG_HANDSHAKE_LENGTH = 20 + 8 + 20 + 20;

    /**
     * size of the receive buffer, it MUST be more than
     * size of the max allowed PIECE message plus some
     * operational space for small messages..
     * but seems it's best to size it to allow receive several
     * small messages and one PIECE inside first part (normal mode).
     * that could work well when pieces are mixed with small messages
     */
    public static final int RECV_BUFFER_SIZE = 256 + 2 * PIECE_BLOCK_MAX_SIZE;
    public static final int SEND_BUFFER_SIZE = 256 + 2 * PIECE_BLOCK_MAX_SIZE;

    /**
     * max allowed space of the receive buffer to be used in normal mode,
     * tail space must allow writing of max (PIECE - 1)
     */
    public static final int RECV_BUFFER_NORMAL_LIMIT = RECV_BUFFER_SIZE - PIECE_BLOCK_MAX_SIZE - MSG_PIECE_PREFIX_LENGTH;
    public static final int SEND_BUFFER_NORMAL_LIMIT = SEND_BUFFER_SIZE - PIECE_BLOCK_MAX_SIZE - MSG_PIECE_PREFIX_LENGTH;

    /**
     * amount of data in buffer to allow it's compaction (copy data to the beginning),
     * mostly to copy small parts of data
     */
    public static final int SEND_BUFFER_COMPACT_LIMIT = 128; // could be == MSG_PIECE_PREFIX_LENGTH;



    /**
     * ref to the parent peer
     */
    Peer peer;

    /**
     * channel to communicate with remote peer
     */
    SocketChannel channel;
    /**
     * selection key, registered for notifications
     */
    SelectionKey sKey;

    /**
     * buffer used to send messages via channel,
     * default state is the following:
     *  - position points to start of the data ready to be sent
     *  - limit points after the end the prepared data
     *  - (pos, lim) are (0, 0 if buffer is empty
     */
    ByteBuffer sendBuffer;
    /**
     * buffer to write data when receiving from the channel
     * default state is the following (like send buffer):
     *  - position points to start of the data ready to be sent
     *  - limit points after the end the prepared data
     *  - (pos, lim) are (0, 0 if buffer is empty
     *
     * most received messages are small except for PIECE and BITFIELD,
     * but PIECE is the longest... so, only first half of the buffer
     * is used in normal mode, but when start of PIECE message is
     * received, buffer switches to use second half of te buffer too
     * to store linear PIECE
     *
     * all messages are: [4b length][1b type][(length - 1)b data]
     * piece message: [4 + 1 + 4 + 4 + PIECE_BLOCK_MAX_SIZE]
     *
     */
    ByteBuffer recvBuffer;


    // parent torrent we are downloading/uploading pieces for
    Torrent torrent;

    // pieces available at the other end (external peer)
    BitSet peerPieces = new BitSet();

    // "active" queue, REQUEST messages sent to external peer,
    // stored only light messages without data part

    // todo: use something like piecesActive to support 250+ requests
    // or some sort inside
    ArrayList<PeerMessage> blockRequests = new ArrayList<>();

    // "active" queue, REQUEST messages received from external peer,
    // stored only light messages without data part
    ArrayList<PeerMessage> peerBlockRequests = new ArrayList<>();

    // messages to send to external peer when channel is ready,
    // head (first messages) is at the start of the list !!!
    final List<PeerMessage> sendQueue = new ArrayList<>();


    // are we operating in extended mode when
    // whole receive buffer is used to receive tail
    // of a long message (PIECE)
    boolean extendedReceiveBufferMode = false;

    // number of data we must receive to have
    // the full long message (PIECE) in the buffer
    int extendedReceiveBufferModeTailSize = 0;

    // are we using extended mode for send buffer,
    // if yes, all appends are stopped
    // till all data is sent
    boolean extendedSendBufferMode = false;

    /**
     * tracks connected status to allow
     * non-blocking connections establishing
     */
    public boolean connected = false;
    /**
     * tracks if remote handshake message
     * has been received or not, need it to switch
     * to "normal" messages processing
     */
    public boolean handshaked = false;


    /**
     * peer protocol state
     * do we choke the remote side?
     * default state is true due to spec
     */
    boolean choke = true;
    /**
     * peer protocol state
     * are we interested in remote data?
     * default state is false due to spec
     */
    boolean interested = false;
    /**
     * peer protocol state
     * are we choked by the remote side?
     * default state is true due to spec
     */
    boolean peerChoke = true;
    /**
     * peer protocol state
     * is the remote side interested in us?
     * default state is false due to spec
     */
    boolean peerInterested = false;

    /**
     * various per connection statistics
     */
    PeerConnectionStatistics statistics;


    long timeConnected;
    long timeHandshaked;
    boolean bitfieldReceived;



    public PeerConnection(Torrent _torrent, Peer _peer) {
        torrent = _torrent;
        peer = _peer;

        recvBuffer = ByteBuffer.allocateDirect(RECV_BUFFER_SIZE);
        recvBuffer.order(ByteOrder.BIG_ENDIAN);

        sendBuffer = ByteBuffer.allocateDirect(SEND_BUFFER_SIZE);
        sendBuffer.order(ByteOrder.BIG_ENDIAN);

        statistics = new PeerConnectionStatistics(STATISTICS_PERIODS, STATISTICS_PERIOD_LENGTH);

        // reset state to start negotiation
        reset();
    }

    /**
     * resets state of this connection to allow
     * reconnects on errors
     */
    public void reset() {
        // protocol state
        choke = true;
        interested = false;
        peerChoke = true;
        peerInterested = false;

        // connection state
        connected = false;
        handshaked = false;

        // buffers
        sendBuffer.position(0).limit(0);
        recvBuffer.position(0).limit(0);
        extendedReceiveBufferMode = false;
        extendedSendBufferMode = false;

        // external state
        peerPieces.clear();

        // queues
        blockRequests.clear();
        sendQueue.clear();

        statistics.reset();
        budgetDownload = 0;
        budgetUpload = 0;
    }



    /**
     * called periodically to update state of the connection,
     * performs initial setup after opening a connection,
     * finishes connecting phase, maintains handshakes and initial messages,
     * allocates
     */
    void updateConnection() {
        // initiate / finish connect
        if (!connected) {
            try {
                boolean connected = connect();
                if (connected) {
                    // send our handshake directly as it doesn't fit into peer message
                    // and there is always space in send buffer on new connection
                    sendHandshake();

                    // the 1st message must be bitfield, but only
                    // in case if we have at least one block
                    if (0 < torrent.pieces.cardinality()) {
                        enqueue(torrent.pmCache.bitfield((int)torrent.metainfo.pieces, torrent.pieces));
                    }
                    /*
                    if (client.node != null) {
                        pc.enqueue(PeerMessage.port(client.node.port));
                    }
                    */
                }
            } catch (IOException e) {
                assert true: "peer: " + peer.address + "  io exception, setting error [1], " + e.getMessage();
                peer.setConnectionClosed(true);
            }
            // connection will be processed on the next turn
            return;
        }

        // wait for handshake to be received,
        // statuses updated in PeerConnection.onXxx
        if (!handshaked) {
            return;
        }


        // check if we have block requests without
        // an answer from eternal side, cancel them
        // and notify parent torrent to re-allocate them
        cancelTimedOutBlockRequests();


        if (choke) {
            // allow all external peers to download from us
            enqueue(torrent.pmCache.unchoke());
        }

        // todo: run keep alive

        // seed mode, decide if we still want to upload
        // to this connection (logic placed in Torrent#onRequest)
        if (torrent.completed) {
            if (interested) {
                enqueue(torrent.pmCache.notInterested());
            }
            return;
        }

        //
        // download / upload mode
        if (torrent.hasMissingPieces(this)) {
            // peer has pieces missing on our side
            if (!interested) {
                // indicate our interest if not at the moment
                enqueue(torrent.pmCache.interested());
            }

            // enqueue blocks to download
            enqueueBlockRequests();

            return;
        }
        else {
            // remote peer doesn't have pieces we are interested in
            if (!peerInterested) {
                // other side not interested in us too, disconnect
                close();
                return;
            }

            if (interested) {
                // sent we are not interested in peer
                enqueue(torrent.pmCache.notInterested());
                return;
            }
        }
    }

    /**
     * allocates new pieces and blocks to be requested, controls per connection speed limits,
     */
    private void enqueueBlockRequests()
    {
        if ( peerChoke           // we are choked
                || !interested   // we don't have interest in peer
        ) {
            return;
        }

        int queueSize = getActiveBlockRequestsNumber() + getEnqueuedBlockRequestsNumber();
        if (queueSize < DOWNLOAD_QUEUED_REQUESTS_MAX) {
            int requests = DOWNLOAD_QUEUED_REQUESTS_MAX - queueSize;

            // use speed limit if specified
            if (budgetDownload > 0) {
                long last = statistics.download.last();
                int allowed = (int) (budgetDownload - last + Torrent.BLOCK_LENGTH - 1) >> Torrent.BLOCK_LENGTH_BITS;
                requests = Math.min(allowed, requests);
            }

            if (requests <= 0) {
                return;
            }

            int allocated = torrent.enqueueBlockRequests(this, requests);
            if (allocated == 0) {
                // seems this connection has no more data for us
//                if (torrent.completed) {
                    enqueue(torrent.pmCache.notInterested());
//                }
            }
        }
    }

    /**
     * checks collection of block requests being active
     * for timeout and cancels them to allow Torrent
     * to resend them later,
     * must be called periodically
     */
    public void cancelTimedOutBlockRequests() {
        long now = System.currentTimeMillis();
        for (int i = blockRequests.size() - 1; 0 <= i; i--) {
            PeerMessage pm = blockRequests.get(i);
            if (DOWNLOAD_QUEUE_REQUESTS_TIMEOUT < now - pm.timestamp) {
                System.out.println("-Xo " + pm.index + "  " + (pm.begin >> 14));
                blockRequests.remove(i);
                torrent.cancelBlockRequest(pm.index, pm.begin);
                torrent.pmCache.release(pm);
            }
        }
    }

    /**
     * NOTE: method is for internal use and debug only,
     * doesn't use any synchronization
     * @return number of active (sent to external peer) block requests
     */
    public int getActiveBlockRequestsNumber() {
        return blockRequests.size();
    }

    /**
     * NOTE: method is for internal use and debug only,
     * doesn't use any synchronization
     * @return number of enqueued block requests
     */
    public int getEnqueuedBlockRequestsNumber() {
        int count = 0;
        for (int i = 0; i < sendQueue.size(); i++) {
            PeerMessage pm = sendQueue.get(i);
            if (pm.type == PeerMessage.REQUEST) {
                count++;
            }
        }
        return count;
    }

    /**
     * performs connection to the external peer and sends initial handshake
     * @return true if connection established and false if still pending
     * @throws IOException if any
     */
    boolean connect() throws IOException {

        if (channel == null) {
            // initiate connect
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            connected = channel.connect(peer.address);
            return connected;
        }

        if (!connected) {
            // channel was created but connection is pending,
            // try to finish it
            connected = channel.finishConnect();
            if (connected) {
                // ok, connection has been established,
                // initiate registration and other actions
                Selector selector = torrent.getSelector();
                // registration is internally synchronized
                sKey = channel.register(selector, SelectionKey.OP_READ /*| SelectionKey.OP_WRITE*/, this);
                // sore time for connection management
                timeConnected = System.currentTimeMillis();
            }
        }

        return connected;
    }

    /**
     * cancels selection key to stop receiving notifications
     * and closes the channel, notifies torrent about peer disconnect
     */
    public void close() {
        // notify parent torrent to cancel requests that are
        // enqueued and that have been already sent to the remote peer
        Stream.concat(sendQueue.stream(), blockRequests.stream())
                .filter(pm -> pm.type == PeerMessage.REQUEST)
                .forEach(pm -> torrent.cancelBlockRequest(pm.index, pm.begin));

        // release messages
        sendQueue.forEach( pm -> torrent.pmCache.release(pm) );
        sendQueue.clear();

        blockRequests.forEach( pm -> torrent.pmCache.release(pm) );
        blockRequests.clear();

        try {
            if (sKey != null) {
                sKey.cancel();
            }
            if (channel != null) {
                channel.close();
            }
        } catch (IOException ignored) {}


        peer.setConnectionClosed(true);
        torrent.onPeerDisconnect(this);
    }

    boolean accept() throws IOException {
        // wait for hs
        // send hs
        return true;
    }


    /**
     * low level operation, performs the following steps:
     * - sends prepared data from the send buffer to channel
     * - clears the buffer if everything is sent or compacts if not
     *
     * on io errors calls {@link PeerConnection#close()} to drop the connection
     * and notify parent torrent
     *
     * buffer state:
     *  on call: position and limit must point to the prepared data
     *  on exit: position and limit will point to existing data or (0, 0) - no data
     *
     * @return number of bytes sent
     */
    private int sendPreparedSendBuffer()
    {
        try {
            int n = channel.write(sendBuffer);

            // track upload speed
            statistics.upload.add(n);

            if (sendBuffer.position() == sendBuffer.limit()) {
                // all data gone, reset buffer to the default state
                sendBuffer.position(0);
                sendBuffer.limit(0);
                // reset extended mode if active
                extendedSendBufferMode = false;

                // nothing more to send, switch off
                // WRITE events
                sKey.interestOpsAnd(~SelectionKey.OP_WRITE);
            }
            else {
                // portion of data left, check if it's small
                // enough to compact
                if (sendBuffer.remaining() < SEND_BUFFER_COMPACT_LIMIT) {
                    sendBuffer.compact();
                    // if we only use start of the buffer, switch off
                    // extended mode for (the check is really not necessary,
                    // more like protection against incorrect buffer configuration )
                    if (sendBuffer.limit() < SEND_BUFFER_NORMAL_LIMIT) {
                        extendedSendBufferMode = false;
                    }
                }
            }
            return n;
        } catch (IOException e) {
            assert true: peer.address + " error [1], " + e.getMessage();
            close();
            return 0;
        }
    }

    /**
     * sends standard handshake when connection initiated,
     * separate method to not overload peer message class with extra fields
     * that used only once..
     * called to send the 1st message so buffer is guarantied to be empty
     * and has enough space for the handshake
     */
    private void sendHandshake() {
        // set up buffer to append data,
        // (that must be the start of the buffer as this is the 1st message)
        int position = sendBuffer.position();
        sendBuffer.position(sendBuffer.limit());
        sendBuffer.limit(sendBuffer.capacity());

        PeerProtocol.populateHandshake(sendBuffer, torrent.getMetainfo().getInfohash(), torrent.getClient().id);

        // make all data (previous and new) available
        sendBuffer.limit(sendBuffer.position());
        sendBuffer.position(position);

        // send and reset buffer
        sendPreparedSendBuffer();
    }

    /**
     * renders send queue into the send buffer and performs send operation,
     * buffer is cleared or compacted on exit to be in "default internal state",
     *
     * when  {@link PeerMessage#PIECE} is processed, parent torrent/storage notified
     * to unlock blocks of data after serialization into write buffer.
     * see {@link PeerConnection#sendPreparedSendBuffer()}
     *
     */
    private void sendFromSendQueue()
    {
        // here buffer has position == beginning of the data
        //                    limit == end of the data

        if (extendedSendBufferMode) {
            // we are using used upper part of the send buffer,
            // wait till all data is sent
            sendPreparedSendBuffer();
            return;
        }

        // set up buffer to append data
        int position = sendBuffer.position();
        sendBuffer.position(sendBuffer.limit());
        sendBuffer.limit(sendBuffer.capacity());

        // append new messages
        int processed = 0;
        for (PeerMessage message: sendQueue) {
            // we always have enough free space in the buffer
            // as we reserve tail space, but just allow checks in populate
            boolean populated = PeerProtocol.populate(sendBuffer, message);
            if (!populated) {
                // this must drop the connection
                // due to unrecoverable error
                assert true: peer.address + " error [2]";
                close();
                return;
            }

            // count processed messages
            processed += 1;


            if (message.type == PeerMessage.REQUEST) {
                // track active block requests from our side,
                // need to maintain max number of simultaneous requests
                message.timestamp = System.currentTimeMillis();
                blockRequests.add(message);
            }
            else if (message.type == PeerMessage.PIECE) {
                // let torrent to unlock block of data
                // and return it into cache of blocks
                torrent.releaseBlock(message);
                torrent.pmCache.release(message);
            }
            else if (message.type == PeerMessage.CHOKE) {
                // remove all active requests from peer,
                // seems we are not going to answer them
                peerBlockRequests.forEach(torrent.pmCache::release);
                peerBlockRequests.clear();
            }
            else if (message.type == PeerMessage.CANCEL) {
                // check for the active linked request and remove it
                for (int i = 0; i < blockRequests.size(); i++) {
                    PeerMessage pm = blockRequests.get(i);
                    if ((pm.index == message.index)
                            && (pm.begin == message.begin)
                            && (pm.length == message.length))
                    {
                        blockRequests.remove(i);
                        torrent.pmCache.release(pm);
                        break;
                    }
                }
            } else {
                // return message to cache, it's still present in the queue,
                // but will be removed right after the cycle
                torrent.pmCache.release(message);
            }

            // protection to flush large messages
            if (SEND_BUFFER_NORMAL_LIMIT < sendBuffer.position()) {
                extendedSendBufferMode = true;
                break;
            }
        }
        // here index points to last processed message
        sendQueue.subList(0, processed).clear();


        // revert buffer to contain all data
        sendBuffer.limit(sendBuffer.position());
        sendBuffer.position(position);

        // send & reset buffer
        sendPreparedSendBuffer();
    }

    /**
     * add another message to the send queue for sending when channel is ready,
     * notifies parent torrent if there are enqueued blocks that could be unlocked,
     * first message is at index [0]
     *
     * See class implementation comments for queue<-->buffer processing details.
     *
     * @param message message to send
     */
    void enqueue(PeerMessage message)
    {
        synchronized (sendQueue) {
            if (message.type == PeerMessage.CHOKE)
            {
                // remove reply messages with piece (block) data
                // from the send queue (due to spec)
                for (int i = sendQueue.size() - 1; 0 <= i; i--) {
                    PeerMessage pm = sendQueue.get(i);
                    if (pm.type == PeerMessage.PIECE) {
                        sendQueue.remove(i);
                        torrent.releaseBlock(pm);
                        torrent.pmCache.release(pm);
                    }
                }
                // set choke status
                choke = true;
                // send choke asap
                sendQueue.add(0, message);
            }
            else if (message.type == PeerMessage.CANCEL)
            {
                // remove the linked request if it's still in the queue
                for (int i = sendQueue.size() - 1; 0 <= i; i--) {
                    PeerMessage pm = sendQueue.get(i);
                    if ((pm.type == PeerMessage.REQUEST)
                            && (pm.index == message.index)
                            && (pm.begin == message.begin)
                            && (pm.length == message.length)) // this check is redundant as length is fixed
                    {
                        sendQueue.remove(i);
                        torrent.pmCache.release(pm);
                        // don't really enqueue CANCEL as the linked request
                        // has been found and removed
                        torrent.pmCache.release(message);
                        return;
                    }
                }
                // send asap
                sendQueue.add(0, message);
            }
            else {
                if (message.type == PeerMessage.INTERESTED) {
                    interested = true;
                }
                else if (message.type == PeerMessage.NOT_INTERESTED) {
                    interested = false;
                }
                else if (message.type == PeerMessage.UNCHOKE) {
                    choke = false;
                }

                // enqueue message
                sendQueue.add(message);
            }

            // indicate we are ready to send data via channel
            if (!sendQueue.isEmpty()) {
                sKey.interestOpsOr(SelectionKey.OP_WRITE);
            }
        }
    }


    /**
     * called when channel is ready to read data,
     * reads data into the buffer and starts parsing and processing
     * of only fully received messages.
     * calls {@link PeerConnection#close()} to drop the connection if read or parsing fails
     */
    private void receive() {

        // make alias..
        // here buffer is configured as:
        //   position: beginning of previous data received or zero if empty
        //      limit: end of a data in buffer or zero
        ByteBuffer rb = recvBuffer;

        // configure buffer to append data
        int position = rb.position();
        rb.position(rb.limit());
        if (!extendedReceiveBufferMode) {
            // in normal mode we use only part of the buffer
            // see {@link Peer#RECV_BUFFER_NORMAL_LIMIT}
            rb.limit(RECV_BUFFER_NORMAL_LIMIT);
        } else {
            // in extended mode allow to receive
            // tail of the last long message,
            // mode is switched off only after successful receive
            rb.limit(rb.position() + extendedReceiveBufferModeTailSize);
        }

        // read
        int n;
        try {
            n = channel.read(recvBuffer);
        } catch (IOException e) {
            // drop the connection
            assert true: peer.address + " error [3], " + e.getMessage();
            close();
            return;
        }

        if (n == -1) {
            // end of stream, close connection
            // that could be connection close in case of seed-seed
            assert true: peer.address + " error [4]";
            close();
            return;
        }

        if (n == 0) {
            // not data available
            return;
        }

        // track download speed
        statistics.download.add(n);

        // revert buffer to default state to work with data
        rb.limit(rb.position());
        rb.position(position);

        if (!handshaked) {
            // still waiting for handshake, check if we have enough data
            if (rb.remaining() < MSG_HANDSHAKE_LENGTH) {
                // wait for the next turn
                return;
            } else {
                // enough data, parse handshake
                boolean correct = PeerProtocol.processHandshake(torrent, this, rb);
                if (!correct) {
                    System.out.println(peer.address + " error [5]");
                    close();
                    return;
                }
            }
        }


        // parse messages in receive buffer
        while (true) {
            int msgStartPosition = rb.position();

            // check if have at least message length field and
            // wait for the next read cycle if not
            int remaining = rb.remaining();
            if (remaining < 4) {
                break;
            }

            // only peek data for a case of a truncated message
            int len = rb.getInt(msgStartPosition);

            // this is remaining minus length field (4 bytes),
            // only data remaining part
            remaining = rb.remaining() - 4;

            // check if we don't have full message in the buffer,
            // we'll need to exit cycle and setup buffer for the next turn
            if (remaining < len) {
                // MSG_PIECE_PREFIX_LENGTH is just the size of the longest prefix, could be 1024
                // this must be PIECE (or long BITFIELD, theoretically)
                if (MSG_PIECE_PREFIX_LENGTH < len) {
                    extendedReceiveBufferMode = true;
                    extendedReceiveBufferModeTailSize = len - remaining;
                }
                break;
            }

            // here we have full message in the buffer, set position,
            // setup limit for message processing (there could be other messages after this one) and process
            // (keep-alive with zero length goes here too)
            int limit = rb.limit();
            rb.limit(msgStartPosition + 4 + len);
            rb.position(msgStartPosition + 4);
            boolean correct = PeerProtocol.processMessage(torrent, this, rb, len);
            rb.limit(limit);
            if (!correct) {
                System.out.println(peer.address + " error [6]");
                close();
                return;
            }
            // reset extended mode if active
            extendedReceiveBufferMode = false;
        }

        // check if not all the received data has been processed
        // and something is left in the buffer
        if (rb.position() == rb.limit()) {
            // we have processed the whole buffer, no data left...
            // just reset the buffer to start from the beginning
            rb.position(0);
            rb.limit(0);

            // extendedReceiveBufferMode = false; // must not be true here
        }
        else if (rb.limit() - rb.position() <= MSG_PIECE_PREFIX_LENGTH) {
            // only small data in the buffer (not like half of PIECE message),
            // so compact the buffer
            rb.compact();
            rb.limit(rb.position());
            rb.position(0);
        }
    }



    /**
     * called when peer's channel is ready for IO operations
     */
    void onChannelReady() {
        if (sKey.isReadable()) {
            receive();
        }
        // key could become cancelled while handling receive,
        // connections could be dropped or io error raised
        if (sKey.isValid() && sKey.isWritable()) {
            sendFromSendQueue();
        }
    }


    /**
     * onXxx methods are called when appropriate message has been received
     * from the external peer, processing started in {@link #receive()}
     */

    /**
     * called on KEEP ALIVE messages
     */
    void onKeepAlive() {}

    /**
     * called after receiving of handshake message from the peer
     * @param reserved reserved bytes
     * @param torrentId id of the torrent requested
     * @param peerId peer identifier
     */
    void onHandshake(byte[] reserved, HashId torrentId, HashId peerId) {
        handshaked = true;
        timeHandshaked = System.currentTimeMillis();
        System.out.println(peer.address + "  handshaked");
    }

    /**
     * called on PORT message receive, usually right after the BITFIELD message
     * @param port DHT port of the peer
     */
    void onPort(int port) {
        // seems we don't need sync here,
        // as atomicity is guaranteed and
        // sooner or later value will be available
        peer.dhtPort = port;
    }

    /**
     * called on "request" message receive,
     * simply notifies parent torrent to find data and enqueue it for sending
     * @param index  piece index
     * @param begin block position inside the piece
     * @param length length of the block
     */
    void onRequest(int index, int begin, int length)
    {
        // validate common block parameters
        boolean correct = torrent.validateBlock(index, begin, length);
        if (!correct) {
            // just ignore such requests
            statistics.blocksRequestedIncorrect++;
            return;
        }

        // this could be moved to send(), but doesn't really matter
        statistics.blocksSent++;

        // track active requests
        PeerMessage pm = torrent.pmCache.request(index, begin, length);
        peerBlockRequests.add(pm);

        // notify torrent to read and enqueue block
        torrent.onRequest(this,  index, begin, length);
    }

    /**
     * called on successful data block receive (piece message),
     * clears linked requests from active queue {@link PeerConnection#blockRequests}
     * and notifies parent torrent to store data block
     * @param buffer buffer with the data received, position is set at
     *               the beginning of the data block, "length" bytes MUST be read from it
     * @param index  piece index
     * @param begin block position inside the piece
     * @param length length of the block
     */
    void onPiece(ByteBuffer buffer, int index, int begin, int length)
    {
        statistics.blocksReceived++;

        // validate common block parameters
        boolean correct = torrent.validateBlock(index, begin, length);
        if (!correct) {
            // drop the connection
            close();
            return;
        }

        // remove from active requests (must be only 1),
        // but it's possible to receive correct block after
        // CHOKE or CANCEL so it will be missing in #blockRequests
        for (int i = 0; i < blockRequests.size(); i++) {
            PeerMessage pm = blockRequests.get(i);
            if ((pm.index == index)
                    && (pm.begin == begin)
                    && (pm.length == length))
            {
                blockRequests.remove(i);
                torrent.pmCache.release(pm);
                break;
            }
        }

        // add another request for this peer,
        // could be add by #update on the next turn,
        // but this could send request on this IO turn
        enqueueBlockRequests();

        // notify torrent to read & process the data
        torrent.onPiece(this, buffer, index, begin, length);
    }

    /**
     * called on "choke" and "unchoke" messages receive,
     * notifies parent torrent about cancelled requests if enqueued or already sent
     * at the moment and message is "choke"
     * @param state true if it was "choke" and false otherwise
     */
    void onChoke(boolean state) {
        peerChoke = state;
        System.out.println(peer.address + " choke: " + state);

        if (peerChoke) {
            // remove all enqueued requests as they will be dropped by the remote peer
            for (int i = sendQueue.size() - 1; 0 <= i; i--) {
                PeerMessage pm = sendQueue.get(i);
                if (pm.type == PeerMessage.REQUEST) {
                    sendQueue.remove(i);
                    // notify torrent about cancelled request
                    torrent.cancelBlockRequest(pm.index, pm.begin);
                    torrent.pmCache.release(pm);
                }
            }
            // it's possible that we have requests that were already sent to external peer,
            // notify torrent - they are cancelled too [who knows, maybe data will arrive]
            for (int i = 0; i < blockRequests.size(); i++) {
                PeerMessage pm = blockRequests.get(i);
                torrent.cancelBlockRequest(pm.index, pm.begin);
                torrent.pmCache.release(pm);
            }
            blockRequests.clear();
        }
    }

    /**
     * called on "cancel" message receive,
     * removes correspondent enqueued message from the send queue if exists
     * and notifies parent torrent to unlock the linked block of data (if buffered somehow)
     * @param index  piece index
     * @param begin block position inside the piece
     * @param length length of the block
     */
    void onCancel(int index, int begin, int length)
    {
        // validate common block parameters
        boolean correct = torrent.validateBlock(index, begin, length);
        if (!correct) {
            close();
            return;
        }

        // check if are processing this request right now
        for (int i = 0; i < peerBlockRequests.size(); i++) {
            PeerMessage pm = peerBlockRequests.get(i);
            if ((pm.index == index)
                    && (pm.begin == begin)
                    && (pm.length == length))
            {
                sendQueue.remove(i);
                torrent.releaseBlock(pm);
                torrent.pmCache.release(pm);
                break;
            }
        }

        // check if response has been enqueued,
        // could be skipped if not found in pBR
        for (int i = 0; i < sendQueue.size(); i++) {
            PeerMessage pm = sendQueue.get(i);
            if ((pm.type == PeerMessage.PIECE)
                    && (pm.index == index)
                    && (pm.begin == begin)
                    && (pm.length == length))
            {
                sendQueue.remove(i);
                torrent.releaseBlock(pm);
                torrent.pmCache.release(pm);
                break;
            }
        }
    }

    /**
     * called on "interested" and "not_interested" messages receive,
     * notifies parent torrent to unlock blocks of data enqueued for sending (if buffered somehow)
     * @param state true if it was "interested" and false otherwise
     */
    void onInterested(boolean state)
    {
        peerInterested = state;

        if (!peerInterested) {
            // remove all piece if enqueued,
            // most likely will not happen
            for (int i = sendQueue.size() - 1; 0 <= i; i--) {
                PeerMessage pm = sendQueue.get(i);
                if (pm.type == PeerMessage.PIECE) {
                    sendQueue.remove(i);
                    torrent.releaseBlock(pm);
                    torrent.pmCache.release(pm);
                }
            }
        }
    }

    /**
     * called on "have" message receive,
     * marks correspondent piece as available on peer side
     * @param index piece index
     */
    void onHave(int index)
    {
        // in correct piece we must have correct block #0
        boolean correct = torrent.validateBlock(index, 0, 0);
        if (!correct) {
            close();
            return;
        }

        peerPieces.set(index);
    }

    /**
     * called on "BITFIELD" message receive,
     * marks correspondent pieces as available on peer side
     * @param mask bitset with 1 for pieces available
     */
    void onBitField(BitSet mask)
    {
        // BITFIELD only allowed once
        if (bitfieldReceived) {
            close();
            return;
        }

        peerPieces.clear();
        peerPieces.or(mask);
        bitfieldReceived = true;
    }



    long budgetDownload;
    long budgetUpload;

    public PeerConnectionStatistics getStatistics() {
        return null;
    }

    public long getDownloadSpeed(int seconds) {
        return statistics.download.average(seconds);
    }
    public long getUploadSpeed(int seconds) {
        return statistics.upload.average(seconds);
    }
}
