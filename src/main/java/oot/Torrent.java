package oot;

import oot.be.Metainfo;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.*;

public class Torrent {

    /**
     * internal states of each torrent
     */
    enum State {
        /**
         * state is unknown, usually after creation of a new torrent,
         * must to load state, checks files, bind file channels, etc.
         */
        UNKNOWN,
        /**
         * initialization is in progress, querying trackers, binding to files,
         * checking state, etc.
         */
        INITIALIZING,
        /**
         * initialization finished, could perform some action
         * on the main thread
         */
        INITIALIZED,
        /**
         * torrent is downloading, seeding, etc.
         */
        ACTIVE,
        /**
         * opened connections could stay, keep alive sent
         * todo: review
         */
        PAUSED,
        /**
         * bind to files, all information is known, could updated with
         * new peers from dht, etc.
         * connections to peers are closed.
         */
        STOPPED,
        /**
         * something is wrong
         */
        ERROR
    }

    /**
     * max number of opened connections while downloading
     * the torrent
     */
    public static final int CONNECTIONS_MAX_DOWNLOAD = 4;
    /**
     * max number of connection is seed mode
     */
    public static final int CONNECTIONS_MAX_SEED = 2;

    /**
     * Size of blocks requested from remote peers,
     * must be power of 2, 16Kb by the spec
     */
    public static final int BLOCK_LENGTH = 16384;
    /**
     * Mask of block size bits
     */
    public static final int BLOCK_LENGTH_MASK = BLOCK_LENGTH - 1;
    /**
     * number of bits used to represent length of a block
     */
    public static final int BLOCK_LENGTH_BITS = Integer.bitCount(BLOCK_LENGTH_MASK);

    /**
     * ref to parent client that controls all the torrents,
     * dht node and other staff
     */
    private Client client;

    /**
     * metainfo of the torrent, parsed ".torrent" file
     */
    Metainfo metainfo;


    // number of blocks in piece, piece / 16K
    final int pieceBlocks;

    /**
     * list of all known peers, possibly not accessible,
     * populated from torrent metainfo, DHT, peer exchange, etc.
     */
    final Set<Peer> peers = new HashSet<>();
    /**
     * utility collection used to add new peers into the main
     * peers collection from other threads
     */
    private final Set<Peer> peersSyncAdd = new HashSet<>();

    /**
     * active peer connections that are working right now
     */
    Map<Peer, PeerConnection> connections = new HashMap<>();


    /**
     * lock instance to sync access to pieces state,
     * active pieces, cache, etc.
     * could be replaces with fine grained locks
     */
    private final Object piecesConfigurationLock = new Object();

    /**
     * state of pieces available on our size,
     * includes only pieces we have fully downloaded
     */
    final BitSet pieces;

    /**
     * describes status of a piece dividing it into blocks
     * downloaded separately
     */
    static class PieceBlocks implements Serializable {
        /**
         * time of the last update
         */
        long timestamp;
        /**
         * stores completed blocks
         */
        BitSet ready;
        /**
         * blocks that are being downloaded (requested)
         */
        BitSet active;

        public PieceBlocks(int blocks) {
            ready = new BitSet(blocks);
            active = new BitSet(blocks);
            reset();
        }

        public void reset() {
            timestamp = System.currentTimeMillis();
            ready.clear();
            active.clear();
        }
    }

    /**
     * status of all pieces being downloaded,
     * Map<piece index, status>
     */
    final Map<Integer, PieceBlocks> piecesActive = new HashMap<>();
    /**
     * cache of instances to be reused and not recreated
     */
    private final ArrayDeque<PieceBlocks> pieceBlocksCache = new ArrayDeque<>();

    /**
     * per-torrent storage api
     */
    private Storage.TorrentStorage storage;

    /**
     * global state of this torrent
     */
    volatile State state;


    /**
     * do we have all the data of this torrent or not,
     * must be based on pieces.cardinality()
     * todo: remove and check cardinality???
     */
    volatile boolean completed;


    List<Tracker> trackers;

    /**
     * cache of PeerMessage instances...
     * they are short lived objects and could be handled by GC,
     * but this work well too
     */
    PeerMessageCache pmCache = new PeerMessageCache();


    private Torrent(Client _client, Metainfo _metainfo) {
        client = _client;
        metainfo = _metainfo;
        pieceBlocks = (int)(metainfo.pieceLength >> BLOCK_LENGTH_BITS);
        state = State.UNKNOWN;
        pieces = new BitSet((int)metainfo.pieces);

        trackers = new ArrayList<>();
        for (int i = 0; i < metainfo.trackers.size(); i++) {
            List<String> urls = metainfo.trackers.get(i);
            Tracker tracker = new Tracker(this, urls);
            trackers.add(tracker);
        }
    }

    public Torrent(Client _client, Metainfo _metainfo, Storage.TorrentStorage _storage) {
        this(_client, _metainfo);
        storage = _storage;
    }

    public Client getClient() {
        return client;
    }

    /**
     * per-torrent selector, but could be only one selector for a client
     * @return
     */
    public Selector getSelector() {
        return getClient().selector;
    }

    public Metainfo getMetainfo() {
        return metainfo;
    }

    public Storage.TorrentStorage getStorage() {
        return storage;
    }



    /**
     * creates new instance of PieceBlocks to be used for
     * piece download, returns instance from the cache or
     * creates new one if cache is empty
     * @return not null instance ready to be used
     */
    private PieceBlocks getPieceBlocksInstance() {
        PieceBlocks pb;
        synchronized (pieceBlocksCache) {
            pb = pieceBlocksCache.pollFirst();
        }
        if (pb == null) {
            pb = new PieceBlocks(pieceBlocks);
        } else {
            pb.reset();
        }
        return pb;
    }

    /**
     * returns used instance of PieceBlocks into cache to be used later
     * @param pb instance
     */
    private void releasePieceBlocks(PieceBlocks pb) {
        synchronized (pieceBlocksCache) {
            pieceBlocksCache.offerFirst(pb);
        }
    }

    /**
     * validate parameters of a block for correctness
     * @param piece piece index
     * @param begin start position inside the piece (in bytes, not blocks)
     * @param length length of the block
     * @return true if parameters are correct
     */
    boolean validateBlock(int piece, int begin, int length) {
        if (metainfo.pieces <= piece) {
            System.out.println("[VLDRB1]  pp:" + metainfo.pieces + "  p:" + piece + " b:" + begin + " l:" + length);
            return false;
        }
        if ((begin & BLOCK_LENGTH_MASK) != 0) {
            System.out.println("[VLDRB2]  pp:" + metainfo.pieces + "  p:" + piece + " b:" + begin + " l:" + length);
            return false;
        }
        if (BLOCK_LENGTH < length) {
            System.out.println("[VLDRB4]  pp:" + metainfo.pieces + "  p:" + piece + " b:" + begin + " l:" + length);
            return false;
        }
        int block = begin >> BLOCK_LENGTH_BITS;
        if (blocks(piece) <= block) {
            System.out.println("[VLDRB3]  pp:" + metainfo.pieces + "  p:" + piece + " b:" + begin + " l:" + length);
            return false;
        }
        return true;
    }


    /**
     * removes specified block from list of active (being downloaded),
     * marks it as finished and checks parent piece for completeness
     * @param piece piece index
     * @param block block index (in blocks, not bytes)
     */
    private void markBlockDownloaded(int piece, int block) {
        synchronized (piecesConfigurationLock) {
            PieceBlocks pb = piecesActive.get(piece);
            if (pb == null) {
                System.out.println("[MBLKD] p:" + piece + " b:" + block);
                return;
            }
            pb.ready.set(block);
            pb.active.clear(block);
            int cardinality = pb.ready.cardinality();
            if (cardinality == blocks(piece)) {
                markPieceDownloaded(piece);
            }
        }
    }

    /**
     * removes block from the list of active (being downloaded) ones
     * to be queried later
     * @param piece piece index
     * @param block block index
     */
    private void markBlockCancelled(int piece, int block) {
        synchronized (piecesConfigurationLock) {
            PieceBlocks pb = piecesActive.get(piece);
            if (pb != null) {
                System.out.println("-X- " + piece + "  " + block + "   E:  r:" + pb.ready.get(block) + " a:" + pb.active.get(block));
                pb.active.clear(block);
            } else {
                System.out.println("[MBLKC]");
                new Exception().printStackTrace();
            }
        }
    }

    /**
     * external api to cancel validated block request
     * @param index piece index
     * @param begin block start position (not block index)
     */
    void cancelBlockRequest(int index, int begin) {
        int block = begin >> BLOCK_LENGTH_BITS;
        markBlockCancelled(index, block);
    }

    /**
     * marks specified piece as finished, release it's status object
     * and returns it into the cache to be reused,
     * calls {@link Torrent#onFinished()} if it was the last piece to be downloaded
     * @param piece piece index
     */
    private void markPieceDownloaded(int piece) {
        synchronized (piecesConfigurationLock) {
            PieceBlocks pb = piecesActive.remove(piece);
            if (pb == null) {
                System.out.println("[MPD1]");
            } else {
                releasePieceBlocks(pb);
            }
            pieces.set(piece);
            int cardinality = pieces.cardinality();
            if (cardinality == metainfo.pieces) {
                onFinished();
            }
        }
    }

    /**
     * checks if the specified connection (peer) has pieces
     * we are interested in, this includes completely missing pieces
     * and the ones being downloaded right now
     * @param pc peer connection to check
     * @return true of there is at least one piece with data for us,
     */
    boolean hasMissingPieces(PeerConnection pc) {
        long p = metainfo.pieces;
        int i = -1;
        synchronized (piecesConfigurationLock) {
            while ((i = pieces.nextClearBit(i + 1)) < p) {
                if (pc.peerPieces.get(i)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * checks if the specified connection (peer) has pieces
     * we are interested in
     * @param pc peer connection to check
     * @param completeOnly only fully downloaded pieces will be considered as interesting
     * @return true of there is at least one piece with data for us,
     */
    private boolean hasMissingPieces(PeerConnection pc, boolean completeOnly) {
        if (!completeOnly) {
            return hasMissingPieces(pc);
        }
        long p = metainfo.pieces;
        int i = -1;
        synchronized (piecesConfigurationLock) {
            while ((i = pieces.nextClearBit(i + 1)) < p) {
                if (pc.peerPieces.get(i) && !piecesActive.containsKey(i)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * calculates number of block in the specified piece,
     * returns only necessary number of blocks for the last piece
     * @param piece piece index
     * @return number of blocks
     */
    private int blocks(int piece) {
        if (piece == metainfo.pieces - 1) {
            long bytesInLastPiece = metainfo.length % metainfo.pieceLength;
            long blocksInLastPiece = (bytesInLastPiece + BLOCK_LENGTH - 1) >> BLOCK_LENGTH_BITS;
            return (int) blocksInLastPiece;
        } else {
            return pieceBlocks;
        }
    }

    /**
     * checks if the specified block is the last one for the torrent
     * @param piece piece index
     * @param block block index inside the piece
     * @return true if block is the last one
     */
    private boolean isLastBlock(int piece, int block) {
        if (piece != metainfo.pieces - 1) {
            return false;
        } else {
            long bytesInLastPiece = metainfo.length % metainfo.pieceLength;
            long blocksInLastPiece = (bytesInLastPiece + BLOCK_LENGTH - 1) >> BLOCK_LENGTH_BITS;
            return block == (blocksInLastPiece - 1);
        }
    }

    /**
     * @return real size in bytes of the last block of the torrent
     */
    private int lastBlockSize() {
        return (int) (metainfo.length & BLOCK_LENGTH_MASK);
    }

    /**
     * allocates specified number of blocks to be requested from the remote peer,
     * blocks are allocated in the way to be available for download
     * from the specified peer.
     * tries to allocate blocks in pieces that are being downloaded already (if available
     * on the specified peer) and allocates new piece if it's necessary and possible
     *
     * @param pc connection that is ready for new requests
     * @param blocks number of blocks to allocate
     * @return number of blocks allocated and requested via the specified connection,
     * zero if there are no more blocks could be requested via the connection
     */
    private int enqueueBlocks(PeerConnection pc, int blocks) {

        int allocated = 0;
        synchronized (piecesConfigurationLock) {

            for (Map.Entry<Integer, PieceBlocks> entry: piecesActive.entrySet()) {
                int piece = entry.getKey();
                PieceBlocks pb = entry.getValue();

                if (pieces.get(piece)) {
                    // already downloaded,
                    // todo: update
                    System.out.println("[ALBL-01]");
                    continue;
                }

                int allocatedInPiece = enqueueBlocks(pc, piece, pb, blocks);
                allocated += allocatedInPiece;
            }

            if (allocated < blocks) {
                // allocate next piece, but choose only that is available
                // at the specified peer
                int piece = enqueuePiece(pc);
                if (piece == -1) {
                    // no more new pieces we can download from
                    // this peer or pieces are over
                    return allocated;
                } else {
                    // allocate more blocks
                    PieceBlocks pb = piecesActive.get(piece);
                    int allocatedInPiece = enqueueBlocks(pc, piece, pb, blocks);
                    allocated += allocatedInPiece;
                }
            }
        }

        return allocated;
    }

    /**
     * allocates specified number of blocks to be requested from remote peers,
     * blocks are allocated in the way to be available for download
     * from the specified peer and inside the only specified piece
     *
     * NOTE: called only from {@link #enqueueBlocks(PeerConnection, int)} inside
     * synchronization block for pieces configuration.
     *
     * @param pc connection that is ready for new requests
     * @param piece piece index to allocate blocks in
     * @param pb status object with information about the specified piece
     * @param amount number of blocks to allocate
     * @return number of blocks allocated and requested via the specified connection,
     * zero if there are no more blocks of the specified piece could be requested via the connection
     */
    private int enqueueBlocks(PeerConnection pc, int piece, PieceBlocks pb, int amount) {
        BitSet ready = pb.ready;
        BitSet active = pb.active;
        int total = blocks(piece);

        int allocated = 0;
        int position = -1;
        while (true) {
            position = ready.nextClearBit(position + 1);
            if (total <=  position) {
                return allocated;
            }
            if (active.get(position)) {
                continue;
            }

            active.set(position);

            int length = isLastBlock(piece, position) ? lastBlockSize() : BLOCK_LENGTH;
            pc.enqueue(pmCache.request(piece, position << BLOCK_LENGTH_BITS, length));

            allocated += 1;
            if (allocated == amount) {
                return allocated;
            }
        }
    }

    /**
     * allocates new piece to be downloaded
     *
     * NOTE: called only from {@link #enqueueBlocks(PeerConnection, int)} inside
     * synchronization block for pieces configuration.
     *
     * @param pc specific peer that is ready for new block requests
     * @return index of the piece allocated or -1 if no more new pieces available,
     * that means last pieces are being downloaded already or the peer has no more pieces
     */
    private int enqueuePiece(PeerConnection pc) {
        int index = -1;

        // todo: allocate random piece

        while (true) {
            synchronized (pieces) {
                index = pieces.nextClearBit(index + 1);
            }
            if (metainfo.pieces <=  index) {
                return -1;
            }

            if (piecesActive.containsKey(index)) {
                continue;
            }
            if (!pc.peerPieces.get(index)) {
                continue;
            }

            // prepare masks and make piece active
            PieceBlocks pb = getPieceBlocksInstance();
            piecesActive.put(index, pb);

            return index;
        }
    }



    long timeLastUpdate = 0;
    long timeLastSaveState = 0;

    /**
     * this method is called periodically by client to update state,
     * open/close new connections, send keep alive messages,
     * perform some maintenance, etc.
     */
    void update() {

        timeLastUpdate = System.currentTimeMillis();

        if (state == State.UNKNOWN) {
            state = State.INITIALIZING;
            storage.init(result -> {
                if (!result) {
                    state = State.ERROR;
                } else {
                    state = State.INITIALIZED;
                }
            });
            return;
        }

        if (state == State.INITIALIZED) {
            restoreState();
            state = State.ACTIVE;
        }

        if (state == State.ACTIVE) {
            updateConnections();
            updateTrackers();

            if (!completed && (5000 < timeLastUpdate - timeLastSaveState)) {
                saveState();
                timeLastSaveState = timeLastUpdate;
            }
        }

        // merge peers collections
        moveNewPeersToMainCollection();
    }

    /**
     * periodically connects to trackers to receive
     * new collections of peers and notify them
     * about out status
     */
    private void updateTrackers() {
        for (Tracker t: trackers) {
            t.update();
        }
    }

    /**
     * performs logic linked to all connections..
     */
    private void updateConnections() {

        // remove connections that were not able to finish
        // connecting phase or were disconnected while
        // processing peer2peer communication
        connections.keySet().removeIf(Peer::isConnectionClosed);

        // todo: use speed limits here to limit connections

        // number of connections we may open
        int toOpen = 0;
        if (completed) {
            if (CONNECTIONS_MAX_SEED < connections.size()) {
                // it's possible we've just stopped download
                // and need to decrease connections
                //closeSlowestConnections(CONNECTIONS_MAX_SEED - connections.size());
            }
            toOpen = CONNECTIONS_MAX_SEED - connections.size();
        } else {
            toOpen = CONNECTIONS_MAX_DOWNLOAD - connections.size();
        }
        while (0 < toOpen--) {
            openConnection();
        }


        connections.forEach((peer, pc) -> pc.updateConnection());
    }

    /**
     * finds another peer in the list of available ones
     * and tries to open connection to it,
     * on success registers new connection in {@link Torrent#connections}
     */
    private void openConnection() {
        for (Peer peer : peers) {
            if (connections.containsKey(peer)) {
                continue;
            }
            if (!peer.isConnectionAllowed()) {
                continue;
            }
            // remove error state as it's possible we are going to reconnect
            peer.setConnectionClosed(false);
            PeerConnection pc = new PeerConnection(this, peer);
            connections.put(peer, pc);

            setBudgetDownload(budgetDownload);

            System.out.println("peer: " + peer.address + "  connection initiated");
            break;
        }
    }


    /**
     * called by a connection to provide more block requests,
     * number of requests controlled by connection itself within
     * the allowed throughput budget
     * @param pc connection that ask for new block requests
     * @param requests number of request connection wants
     * @return number of requests allocated, could be less than requested at the end
     * of download process
     */
    int enqueueBlockRequests(PeerConnection pc, int requests) {
        int allocated = enqueueBlocks(pc, requests);
        while (allocated < requests) {
            int nextPiece = enqueuePiece(pc);
            if (nextPiece == -1) {
                // no more data we are interested in
                break;
            } else {
                allocated += enqueueBlocks(pc, requests - allocated);
            }
        }
        return allocated;
    }



    /**
     * called when data from byte buffer inside peer message has been serialized
     * or buffer will not be used any more, just api wrapper around storage.releaseBuffer
     * @param pm message with block/buffer inside
     */
    void releaseBlock(PeerMessage pm) {
        storage.releaseBuffer(pm.block);
    }


    /**
     * external api method to add new peers to the torrent
     * @param newPeers collection of peers
     */
    public void addPeers(Collection<Peer> newPeers) {
        if (newPeers == null) {
            return;
        }
        synchronized (peersSyncAdd) {
            peersSyncAdd.addAll(newPeers);
        }
    }

    /**
     * internal method to be called on the thread that processes the torrent,
     * moves all new peer from peersSyncAdd collection to the main one,
     * should be called on state update
     */
    private void moveNewPeersToMainCollection() {
        synchronized (peersSyncAdd) {
            peers.addAll(peersSyncAdd);
            peersSyncAdd.clear();
        }
    }

    /**
     * could be called asynchronously from
     * some storage thread
     */
    void onStorageError() {
    }

    long tStart = System.currentTimeMillis();

    /**
     * called from this torrent when last
     * piece of torrent is finished
     */
    void onFinished() {
        System.out.println("FINISHED");
        completed = true;
        saveState();

        long time = System.currentTimeMillis() - tStart;
        System.out.println("time: " + ((double)time)/1000 + "s");
    }


    /**
     * called by a connection when correct block of data received from remote side
     * @param pc connection that has received the data
     * @param buffer buffer with (pos, limit) set to point to data
     * @param index piece index
     * @param begin block position inside the piece
     * @param length length of the block, must be == buffer.remaining()
     */
    void onPiece(PeerConnection pc, ByteBuffer buffer, int index, int begin, int length) {
        storage.write(buffer, index, begin, length, (result) -> {
            // this could be called from some other thread (storage)
            int block = begin >> BLOCK_LENGTH_BITS;
            markBlockDownloaded(index, block);
        });
    }


    /**
     * called by a connection when correct request for a block of data received from remote side
     * @param pc connection that has received the data
     * @param index piece index
     * @param begin block position inside the piece
     * @param length length of the block, must be == buffer.remaining()
     */
    void onRequest(PeerConnection pc, int index, int begin, int length) {
        // todo: speed limits
        // todo: move buffer get to storage ?
        // buffer will be release in send() on message serialization
        final ByteBuffer buffer = storage.getBuffer();
        storage.read(buffer, index, begin, length, result -> {
            PeerMessage pm = pmCache.piece(index, begin, length, buffer);
            pc.enqueue(pm);
        });
    }


    /**
     * called from a connection to notify that physical connection is closed
     * @param pc peer connection
     */
    void onPeerDisconnect(PeerConnection pc) {
        // peer will be removed in update()
        System.out.println(pc.peer.address + " error / disconnected");
        new Exception().printStackTrace();
    }




    long timeLastDump = 0;
    public void dump() {
        timeLastDump = System.currentTimeMillis();

        Formatter formatter = new Formatter();
        formatter.format("                              L  P                                 \n");
        formatter.format("                          C H CI CI  DLR RQ   BLKS |  UPL  Q   BLKS\n");

        connections.forEach((peer, pc) -> {
            PeerConnectionStatistics s = pc.statistics;

            double drate = s.download.average(4);
            drate /= 1024*1024;
            double urate = s.upload.average(4);
            urate /= 1024*1024;

            formatter.format("%24s %2S%2S %1c%1c %1c%1c %4.1f %2d %6d | %4.1f %2d %6d\n",
                    peer.address,
                    pc.connected ? "+" : "-",
                    pc.handshaked ? "+" : "-",

                    pc.choke ? 'c' : '-',
                    pc.interested ? 'i' : '-',
                    pc.peerChoke ? 'c' : '-',
                    pc.peerInterested ? 'i' : '-',

                    drate, pc.blockRequests.size(), s.blocksReceived,
                    urate, 0, s.blocksSent);
        });
        formatter.format(" peer messages created: %d\n", pmCache.counter);
        formatter.format("     buffers allocated: %d\n", SimpleFileStorage.buffersAllocated);
        //formatter.format("       save task queue: %d\n", SimpleFileStorage.exSave.getQueue().size());

        System.out.println(formatter.toString());
    }

    public void getCompletionState(BitSet _pieces, Map<Integer, BitSet> active) {
        _pieces.clear();
        _pieces.or(pieces);

        active.clear();
        piecesActive.forEach( (p, m) -> {
            BitSet tmp = new BitSet(pieceBlocks);
            tmp.or(m.ready);
            active.put(p, tmp);
        });
    }

    public void saveState() {
        storage.writeState(pieces, piecesActive);
    }

    public void restoreState() {
        storage.readState(pieces, piecesActive);
        completed = pieces.cardinality() == metainfo.pieces;
    }


    long budgetDownload;
    long budgetUpload;

    public long getDownloadSpeed(int seconds) {
        long total = 0;
        for (Iterator<PeerConnection> iterator = connections.values().iterator(); iterator.hasNext(); ) {
            PeerConnection pc = iterator.next();
            total += pc.getDownloadSpeed(seconds);
        }
        return total;
    }
    public long getUploadSpeed(int seconds) {
        long total = 0;
        for (Iterator<PeerConnection> iterator = connections.values().iterator(); iterator.hasNext(); ) {
            PeerConnection pc = iterator.next();
            total += pc.getUploadSpeed(seconds);
        }
        return total;
    }


    private long _populateDownloadSpeeds(int seconds, long[] data) {

        long total = 0;
        int index = 0;
        for (Iterator<PeerConnection> iterator = connections.values().iterator(); iterator.hasNext(); ) {
            PeerConnection pc = iterator.next();
            long speed = pc.getDownloadSpeed(seconds);
            data[index++] = speed;
            total += speed;
        }
        return total;
    }

    public static final long XX_AVG_DELTA = 16384;
    public void setBudgetDownload(long budget) {

        budgetDownload = budget;
        if (budget == 0) {
            for (Iterator<PeerConnection> iterator = connections.values().iterator(); iterator.hasNext(); ) {
                PeerConnection pc = iterator.next();
                pc.budgetDownload = 0;
            }
            return;
        }

        if (connections.size() == 0) {
            return;
        }

        long[] speeds = new long[connections.size()];
        long total = _populateDownloadSpeeds(4, speeds);
        long averageBudget = budgetDownload / speeds.length;

        long availableBudget = budgetDownload - total;

        int index = 0;
        int hightSpeedCount = 0;
        for (Iterator<PeerConnection> iterator = connections.values().iterator(); iterator.hasNext(); ) {
            PeerConnection pc = iterator.next();
            long speed = speeds[index++];

            if (speed < averageBudget - XX_AVG_DELTA) {
                // let this torrent to use more bandwidth,
                // soft target is the same speed for all
                pc.budgetDownload = averageBudget;
            } else {
                hightSpeedCount++;
            }
        }

        index = 0;
        for (Iterator<PeerConnection> iterator = connections.values().iterator(); iterator.hasNext(); ) {
            PeerConnection pc = iterator.next();
            long speed = speeds[index++];

            if (averageBudget - XX_AVG_DELTA <= speed) {
                pc.budgetDownload = speed + availableBudget/hightSpeedCount;
            }
        }
    }

}
