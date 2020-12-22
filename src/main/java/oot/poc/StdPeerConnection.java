package oot.poc;

import oot.PeerMessage;
import oot.dht.HashId;

import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Formatter;
import java.util.List;
import java.util.stream.Stream;

class StdPeerConnection extends PeerConnection
{
    // debug switch
    private static final boolean DEBUG = true;

    /**
     * max number of active piece requests (for blocks) could be sent
     * to any external peer (it's 5 by the spec, but should be higher on good connections)
     */
    public static final int DOWNLOAD_QUEUED_REQUESTS_MAX = 11;
    /**
     * timeout for outgoing request to wait for answer,
     * will be dropped and resent on timeout (to the same or
     * any other peer), milliseconds
     * NOTE: highly related to QUEUE size as pieces/blocks
     * could be received too slow
     */
    public static final int DOWNLOAD_QUEUE_REQUESTS_TIMEOUT = 16_000;
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
     * handshake message size
     */
    public static final int MSG_HANDSHAKE_LENGTH = 20 + 8 + 20 + 20;

    // pieces available at the other end (external peer)
    BitSet peerPieces = new BitSet();

    // "active" queue, REQUEST messages sent to external peer,
    // stored only light messages without data part

    // todo: use something like piecesActive to support 250+ requests
    // or some sort inside
    ArrayList<StdPeerMessage> blockRequests = new ArrayList<>();

    // "active" queue, REQUEST messages received from external peer,
    // stored only light messages without data part
    ArrayList<StdPeerMessage> peerBlockRequests = new ArrayList<>();

    // messages to send to external peer when channel is ready,
    // head (first messages) is at the start of the list !!!
    final List<StdPeerMessage> sendQueue = new ArrayList<>();


    /**
     * tracks if remote handshake message
     * has been received or not, need it to switch
     * to "normal" messages processing
     */
    public boolean handshaked = false;
    /**
     * timestamp of the handshake event
     */
    long timeHandshaked;

    /**
     * tracks if bitfield has been received or not
     */
    public boolean bitfieldReceived;

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
    PeerConnectionStatistics statistics = new PeerConnectionStatistics(60, 1000);

    /**
     * download speed limit in bytes/sec or zero if unlimited,
     * this limit is dynamic and managed by parent torrent
     * based on global limits, per torrent limits and current
     * throughput of all connections,
     * used while requesting blocks from external peer
     */
    long speedLimitDownload;
    /**
     * upload speed limit in bytes/sec or zero if unlimited,
     * this limit is dynamic and managed by parent torrent
     * based on global limits, per torrent limits and current
     * throughput of all connections,
     *
     */
    long speedLimitUpload;

    /**
     * ref to instance of messages' cache,
     * could be removed what inline classes are available
     */
    StdPeerMessageCache pmCache;

    public StdPeerConnection(
            Selector _selector,
            Torrent _torrent, Peer _peer,
            ByteBuffer _receiveBuffer,
            int _receiveBufferNormalLimit, int _receiveBufferCompactionLimit,
            ByteBuffer _sendBuffer,
            int _sendBufferNormalLimit, int _sendBufferCompactionLimit,
            StdPeerMessageCache _pmCache)
    {
        super(_selector, _torrent, _peer,
                _receiveBuffer, _receiveBufferNormalLimit, _receiveBufferCompactionLimit,
                _sendBuffer, _sendBufferNormalLimit, _sendBufferCompactionLimit);

        pmCache = _pmCache;
    }


    /**
     * adds another message to the send queue for sending when channel is ready,
     * notifies parent torrent if there are enqueued blocks that could be unlocked,
     * first message is at index [0]
     *
     * See class implementation comments for queue<-->buffer processing details.
     *
     * @param message message to send
     */
    protected void enqueue(StdPeerMessage message)
    {
        if (DEBUG) {
            if (message.type == StdPeerMessage.REQUEST) {
                System.out.println("[stdpc] enqueue: req " + message.index + "  " + (message.begin >> 14));
            } else {
                System.out.println("[stdpc] enqueue: " + message.type);
            }
        }

        if (message.type == StdPeerMessage.CHOKE)
        {
            // remove reply messages with piece (block) data
            // from the send queue (due to spec)
            for (int i = sendQueue.size() - 1; 0 <= i; i--) {
                StdPeerMessage pm = sendQueue.get(i);
                if (pm.type == StdPeerMessage.PIECE) {
                    sendQueue.remove(i);
                    TorrentStorage.Block block = (TorrentStorage.Block) message.params;
                    block.release();
                    pmCache.release(pm);
                }
            }
            // set choke status
            choke = true;
            // send choke asap
            sendQueue.add(0, message);
        }
        else if (message.type == StdPeerMessage.CANCEL)
        {
            // remove the linked request if it's still in the queue
            for (int i = sendQueue.size() - 1; 0 <= i; i--) {
                StdPeerMessage pm = sendQueue.get(i);
                if ((pm.type == StdPeerMessage.REQUEST)
                        && (pm.index == message.index)
                        && (pm.begin == message.begin)
                        && (pm.length == message.length)) // this check is redundant as length is fixed
                {
                    sendQueue.remove(i);
                    pmCache.release(pm);
                    // don't really enqueue CANCEL as the linked request
                    // has been found and removed
                    pmCache.release(message);
                    return;
                }
            }
            // send asap
            sendQueue.add(0, message);
        }
        else {
            if (message.type == StdPeerMessage.INTERESTED) {
                interested = true;
            }
            else if (message.type == StdPeerMessage.NOT_INTERESTED) {
                interested = false;
            }
            else if (message.type == StdPeerMessage.UNCHOKE) {
                choke = false;
            }

            // enqueue message
            sendQueue.add(message);
        }
    }


    @Override
    protected boolean hasEnqueuedDate() {
        return !sendQueue.isEmpty();
    }

    @Override
    void update()
    {
        // call update logic
        _update();
        // and check if there are new messages to be sent
        registerWriteInterest();
    }

    /**
     * internal update logic
     */
    private void _update()
    {
        // let base connection to perform connecting phase
        // and call onConnectionEstablished
        super.update();

        // wait for handshake to be received,
        // connect --> onEstablished --> [send] --> onHandshake
        if (!handshaked) {
            return;
        }

        // check if we have block requests without
        // an answer from external side, cancel them
        // and notify parent torrent to re-allocate them
        cancelTimedOutBlockRequests();


        if (choke) {
            // allow all external peers to download from us
            enqueue(pmCache.unchoke());
        }

        // todo: run keep alive

        // seed mode, decide if we still want to upload
        // to this connection
        if (torrent.completed && interested) {
            enqueue(pmCache.notInterested());
            return;
        }

        // download / upload mode,
        // run quick check for missing pieces (not blocks)
        if (torrent.interested(peerPieces))
        {
            // peer has pieces missing on our side and
            // no connection is downloading them
            if (!interested) {
                // indicate our interest if not at the moment
                enqueue(pmCache.interested());
            }

            // enqueue blocks to download
            enqueueBlockRequests();

            return;
        }
        else {
            // remote peer doesn't have pieces we are interested in,
            // but we could still be receiving requested pieces
            if (!blockRequests.isEmpty()) {
                // wait for blocks to be received or cancelled
                return;
            }

            if (!peerInterested) {
                // other side not interested in us too, disconnect
                close(Peer.CloseReason.NORMAL);
                return;
            }

            if (interested) {
                // sent we are not interested in peer
                enqueue(pmCache.notInterested());
                return;
            }
        }
    }

    /**
     * NOTE: method is for internal use and debug only,
     * doesn't use any synchronization
     * @return number of active (sent to external peer) block requests
     */
    int getActiveBlockRequestsNumber() {
        return blockRequests.size();
    }

    /**
     * NOTE: method is for internal use and debug only,
     * doesn't use any synchronization
     * @return number of enqueued block requests
     */
    int getEnqueuedBlockRequestsNumber() {
        int count = 0;
        for (StdPeerMessage pm : sendQueue) {
            if (pm.type == PeerMessage.REQUEST) {
                count++;
            }
        }
        return count;
    }


    @Override
    boolean isDownloading() {
        return (0 < getActiveBlockRequestsNumber());
    }

    @Override
    BitSet getPeerPieces() {
        return peerPieces;
    }

    /**
     * integration method for parent torrent, allows to make
     * back call with parameters of blocks that should be downloaded
     * by this connection
     * NOTE: not very OOP, but remove extra objects' creation
     *
     * CALLED by parent TORRENT only
     *
     * @param index index of the piece that holds the block
     * @param position position in of the block to request / usually as (block index << 14)
     * @param length length of the block / usually fixed
     */
    @Override
    void enqueueBlockRequest(int index, int position, int length) {
        enqueue(pmCache.request(index, position, length));
    }

    /**
     * integration method for parent torrent, allows to make
     * back calls with data loaded from a storage and ready to be sent to a peer
     *
     * CALLED by parent torrent only
     *
     * @param buffer buffer with data ready to be read
     * @param index index of the piece that holds the block
     * @param position position in of the block to request / usually as (block index << 14)
     * @param length length of the block / usually fixed
     */
    @Override
    void enqueuePiece(ByteBuffer buffer, int index, int position, int length, Object params)
    {
        boolean found = false;

        // check for the active linked peer request and remove it
        for (int i = 0; i < peerBlockRequests.size(); i++) {
            StdPeerMessage pm = peerBlockRequests.get(i);
            if ((pm.index == index)
                    && (pm.begin == position)
                    && (pm.length == length))
            {
                blockRequests.remove(i);
                pmCache.release(pm);
                found = true;
                break;
            }
        }
        if (!found && DEBUG) {
            System.out.println("torrent.enqueuePiece: pBR not found (cancelled?): " + index + " " + position + " " + length);
        }
        enqueue(pmCache.piece(index, position, length, buffer, params));
    }

    /**
     * usually called by connection update to enqueue more block requests,
     * controls per connection speed limits, calculates exact amount
     * of requests needed for this connection and for ask parent torrent
     * to allocates new blocks to be downloaded.
     *
     * NOTE: torrent notifies back via {@link #enqueueBlockRequest(int, int, int)} method
     */
    protected void enqueueBlockRequests()
    {
        if ( peerChoke           // we are choked
                || !interested   // we don't have interest in peer
        ) {
            return;
        }

        // use total number of outgoing requests for planning,
        // (this sum not meant to exceed DOWNLOAD_QUEUED_REQUESTS_MAX)
        int enqueued = getActiveBlockRequestsNumber() + getEnqueuedBlockRequestsNumber();
        if (enqueued < DOWNLOAD_QUEUED_REQUESTS_MAX)
        {
            // number of additional requests we may add to queue
            int requests = DOWNLOAD_QUEUED_REQUESTS_MAX - enqueued;

            // do we have speed limiting on?
            if (speedLimitDownload > 0) {
                // number of bytes sent during the current speed controlled period
                long last = statistics.download.last();
                // number of requests we could send (could be negative),
                // this simply sends request on period start (not evenly distributed)
                int allowed = (int) (speedLimitDownload - last + oot.Torrent.BLOCK_LENGTH - 1) >> oot.Torrent.BLOCK_LENGTH_BITS;

                requests = Math.min(allowed, requests);
            }

            if (requests <= 0) {
                // this handles full queue
                // and speed limit overrun (<0)
                return;
            }

            // ask torrent to allocate more blocks to download
            // via this connection
            int allocated = torrent.enqueueBlockRequests(this, this.peerPieces, requests);
            if (allocated == 0) {
                // seems this connection has no more data for us,
                // but could still have active requests
                if (enqueued == 0) {
                    enqueue(pmCache.notInterested());
                }
            }
        }
    }


    /**
     * checks collection of block requests being active
     * for timeout and cancels them to allow Torrent
     * to resend them later,
     * must be called periodically
     */
    protected void cancelTimedOutBlockRequests()
    {
        long now = System.currentTimeMillis();
        for (int i = blockRequests.size() - 1; 0 <= i; i--) {
            StdPeerMessage pm = blockRequests.get(i);
            if (pm.timestamp + DOWNLOAD_QUEUE_REQUESTS_TIMEOUT < now) {
                System.out.println("-Xo " + pm.index + "  " + (pm.begin >> 14));
                blockRequests.remove(i);
                torrent.cancelBlockRequest(pm.index, pm.begin);
                pmCache.release(pm);
            }
        }
    }

    @Override
    protected void connectionEstablished()
    {
        // reset error flag if we reuse the connection
        peer.resetConnectionClosed();

        // send our handshake directly as it doesn't fit into peer message
        // and there is always space in send buffer on new connection
        enqueue(pmCache.handshake(torrent.getTorrentId(), torrent.getClientId()));

        // the 1st message must be bitfield, but only
        // in case if we have at least one block
        if (0 < torrent.pieces.cardinality()) {
            enqueue(pmCache.bitfield((int)torrent.metainfo.pieces, torrent.pieces));
        }

        /*
        todo
        if (client.node != null) {
            pc.enqueue(StdPeerMessage.port(client.node.port));
        }
        */
    }

    @Override
    protected void connectionClosed(Peer.CloseReason reason)
    {
        // notify parent torrent to cancel requests that are
        // enqueued and that have been already sent to the remote peer
        Stream.concat(sendQueue.stream(), blockRequests.stream())
                .filter(pm -> pm.type == StdPeerMessage.REQUEST)
                .forEach(pm -> torrent.cancelBlockRequest(pm.index, pm.begin));

        // release messages
        sendQueue.forEach( pmCache::release);
        sendQueue.clear();
        // and active requests
        blockRequests.forEach( pmCache::release);
        blockRequests.clear();

        peer.setConnectionClosed(reason);
        torrent.onPeerDisconnect(this);

        // todo: release caches via connectionFactory
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
                    if (DEBUG) System.out.println(peer.address + " error parsing handshake [5]");
                    return -1;
                }
            }
        }

        // parse messages in the buffer
        while (true)
        {
            int msgStartPosition = rb.position();

            // check if have at least message length field and
            // wait for the next read cycle if not
            int remaining = rb.remaining();

            if (remaining == 0) {
                // all messages processed, no more data
                return 0;
            }

            if (remaining < 4) {
                // part of length field
                return 4 - remaining;
            }

            // only peek data for a case of a truncated message
            int len = rb.getInt(msgStartPosition);

            // this is remaining minus length field (4 bytes),
            // only data remaining part
            remaining = rb.remaining() - 4;

            // check if we don't have full message in the buffer,
            // we'll need to exit cycle and setup buffer for the next turn
            if (remaining < len) {
                return len - remaining;
            }

            // here we have full message in the buffer, set position,
            // setup limit for message processing (there could be other messages after this one) and process
            // (keep-alive with zero length goes here too)
            int limit = rb.limit();
            rb.position(msgStartPosition + 4);
            rb.limit(msgStartPosition + 4 + len);
            boolean correct = StdPeerProtocol.processMessage(this, rb, len);

            // restore parameters for stability
            rb.position(msgStartPosition + 4 + len);
            rb.limit(limit);

            if (!correct) {
                if (DEBUG) System.out.println(peer.address + " error parsing p2p protocol message [6]");
                return -1;
            }
        }
    }


    /**
     * renders send queue into the send buffer on request
     *
     * when  {@link StdPeerMessage#PIECE} is processed, parent torrent/storage notified
     * to unlock blocks of data after serialization into write buffer.
     * see {@link PeerConnection#send()}
     *
     */
    @Override
    protected boolean populateSendBuffer(ByteBuffer sb)
    {
        int processed = 0;
        for (StdPeerMessage message: sendQueue)
        {
            boolean populated = StdPeerProtocol.populate(this, sendBuffer, message);
            if (!populated) {
                // indicate we have more data
                return true;
            }

            // count processed messages
            processed += 1;


            if (message.type == StdPeerMessage.REQUEST) {
                // track active block requests from our side,
                // need to maintain max number of simultaneous requests
                message.timestamp = System.currentTimeMillis();
                blockRequests.add(message);
            }
            else if (message.type == StdPeerMessage.PIECE) {
                // let storage to unlock block of data
                // and return it into cache of blocks
                TorrentStorage.Block block = (TorrentStorage.Block) message.params;
                block.release();
                pmCache.release(message);
            }
            else if (message.type == StdPeerMessage.CHOKE) {
                // remove all active requests from peer,
                // seems we are not going to answer them
                peerBlockRequests.forEach(pmCache::release);
                peerBlockRequests.clear();
            }
            else if (message.type == StdPeerMessage.CANCEL) {
                // check for the active linked request and remove it
                for (int i = 0; i < blockRequests.size(); i++) {
                    StdPeerMessage pm = blockRequests.get(i);
                    if ((pm.index == message.index)
                            && (pm.begin == message.begin)
                            && (pm.length == message.length))
                    {
                        blockRequests.remove(i);
                        pmCache.release(pm);
                        break;
                    }
                }
            } else {
                // return message to cache, it's still present in the queue,
                // but will be removed right after the cycle
                pmCache.release(message);
            }
        }

        // here index points to last processed message
        sendQueue.subList(0, processed).clear();

        // all message were rendered into buffer
        return false;
    }



    /**
     * called on KEEP ALIVE messages
     */
    void onKeepAlive() {}

    /**
     * called after receiving of handshake message from the peer
     * @param reserved reserved bytes
     * @param torrent torrent hash identifier
     * @param peerId peer identifier
     */
    void onHandshake(byte[] reserved, HashId torrent, HashId peerId)
    {
        timeHandshaked = System.currentTimeMillis();
        handshaked = true;

        peer.peerId = peerId;
        peer.reserved = reserved;
    }

    /**
     * called on PORT message receive, usually right after the BITFIELD message
     * @param port DHT port of the peer
     */
    void onPort(int port) {
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
        StdPeerMessage pm = pmCache.request(index, begin, length);
        peerBlockRequests.add(pm);

        // notify torrent to read and enqueue block
        torrent.onRequest(this,  index, begin, length);
    }

    /**
     * called on successful data block receive (piece message),
     * clears linked requests from active queue {@link StdPeerConnection#blockRequests}
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
            close(Peer.CloseReason.PROTOCOL_ERROR);
            return;
        }

        // remove from active requests (must be only 1),
        // but it's possible to receive correct block after
        // CHOKE or CANCEL so it will be missing in #blockRequests
        for (int i = 0; i < blockRequests.size(); i++) {
            StdPeerMessage pm = blockRequests.get(i);
            if ((pm.index == index)
                    && (pm.begin == begin)
                    && (pm.length == length))
            {
                blockRequests.remove(i);
                pmCache.release(pm);
                break;
            }
        }

        // notify torrent to read & process the data
        torrent.onPiece(this, buffer, index, begin, length);

        // enqueue new request right now to be sent on this cycle,
        // otherwise it will wait till call to #update()
        enqueueBlockRequests();
    }

    /**
     * called on "choke" and "unchoke" messages receive,
     * notifies parent torrent about cancelled requests if enqueued or already sent
     * at the moment and message is "choke"
     * @param state true if it was "choke" and false otherwise
     */
    void onChoke(boolean state)
    {
        peerChoke = state;
        //if (DEBUG) System.out.println(peer.address + " onChoke: " + state);

        if (peerChoke) {
            // remove all enqueued requests as they will be dropped by the remote peer
            for (int i = sendQueue.size() - 1; 0 <= i; i--) {
                StdPeerMessage pm = sendQueue.get(i);
                if (pm.type == StdPeerMessage.REQUEST) {
                    sendQueue.remove(i);
                    // notify torrent about cancelled request
                    torrent.cancelBlockRequest(pm.index, pm.begin);
                    pmCache.release(pm);
                }
            }
            // it's possible that we have requests that were already sent to external peer,
            // notify torrent - they are cancelled too [who knows, maybe data will arrive]
            for (int i = 0; i < blockRequests.size(); i++) {
                StdPeerMessage pm = blockRequests.get(i);
                torrent.cancelBlockRequest(pm.index, pm.begin);
                pmCache.release(pm);
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
            close(Peer.CloseReason.PROTOCOL_ERROR);
            return;
        }

        // check if are processing this request right now
        for (int i = 0; i < peerBlockRequests.size(); i++) {
            StdPeerMessage pm = peerBlockRequests.get(i);
            if ((pm.index == index)
                    && (pm.begin == begin)
                    && (pm.length == length))
            {
                peerBlockRequests.remove(i);
                pmCache.release(pm);
                break;
            }
        }

        // check if response has been enqueued,
        // could be skipped if found in pBR
        for (int i = 0; i < sendQueue.size(); i++) {
            StdPeerMessage pm = sendQueue.get(i);
            if ((pm.type == StdPeerMessage.PIECE)
                    && (pm.index == index)
                    && (pm.begin == begin)
                    && (pm.length == length))
            {
                sendQueue.remove(i);
                // let storage to unlock block of data
                // and return it into cache of blocks
                TorrentStorage.Block block = (TorrentStorage.Block) pm.params;
                block.release();

                pmCache.release(pm);
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
                StdPeerMessage pm = sendQueue.get(i);
                if (pm.type == StdPeerMessage.PIECE) {
                    sendQueue.remove(i);
                    TorrentStorage.Block block = (TorrentStorage.Block) pm.params;
                    block.release();
                    pmCache.release(pm);
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
            close(Peer.CloseReason.PROTOCOL_ERROR);
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
            if (DEBUG) System.out.println(peer.address + " second bitfield message received, dropping the connection");
            close(Peer.CloseReason.PROTOCOL_ERROR);
            return;
        }

        peerPieces.clear();
        peerPieces.or(mask);

        // update linked peer with completion status
        if (peerPieces.cardinality() == torrent.getMetainfo().pieces) {
            peer.setCompleted(true);
        }

        bitfieldReceived = true;
    }

    /**
     * dumps active connections of the torrent to stdout
     * @param formatter formatter to dump tha state
     */
    @Override
    public void dump(Formatter formatter)
    {
        PeerConnectionStatistics s = rawStatistics;

        double drate = s.download.average(4);
        drate /= 1024*1024;
        double urate = s.upload.average(4);
        urate /= 1024*1024;

        formatter.format("%24s %2S%2S %1c%1c %1c%1c %5.1f %3d %6d | %5.1f %2d %6d  %s\n",
                peer.address,
                connected ? "+" : "-",
                handshaked ? "+" : "-",

                choke ? 'c' : '-',
                interested ? 'i' : '-',
                peerChoke ? 'c' : '-',
                peerInterested ? 'i' : '-',

                drate, blockRequests.size(), s.blocksReceived,
                urate, 0, s.blocksSent,
                StdPeerProtocol.extractClientNameFromId(peer.peerId));
    }
}
