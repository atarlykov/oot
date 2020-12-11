package oot.poc;

import oot.Client;
import oot.be.Metainfo;
import oot.dht.HashId;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class Torrent
{
    // debug switch
    private static final boolean DEBUG = true;

    /**
     * internal states of each torrent
     */
    enum State {
        /**
         * state is unknown, usually after creation of a new torrent,
         * must to load state, checks files, bind file channels, etc.
         */
        NEW,
        /**
         * initialization is in progress, querying trackers, binding to files,
         * checking state, etc.
         */
        INITIALIZING,
        /**
         * torrent is downloading, seeding, etc.
         */
        ACTIVE,
        /**
         * waiting for all active requests to finish,
         * then will switch to STOPPED
         */
        STOPPING,
        /**
         * bound to files, all information is known, could updated with
         * new peers from dht, etc.
         * connections to peers are closed.
         */
        STOPPED,
        /**
         * something is wrong
         */
        ERROR,
    }

    /**
     * max number of opened connections while downloading
     * the torrent
     */
    public static final int CONNECTIONS_DOWNLOAD_MAX = 16;
    /**
     * max number of connection is seed mode
     */
    public static final int CONNECTIONS_SEED_MAX = 2;
    /**
     * timeout to start dropping extra connections after switching to seed mode
     */
    public static final long CONNECTIONS_SEED_DROP_TIMEOUT = 5000L;

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
     * period of torrent state save (pieces downloaded and active) in ms,
     * used only for uncompleted torrents
     */
    public static final long TORRENT_STATE_SAVE_TIMEOUT = 10_000;
    /**
     * period to re-request peers from DHT
     */
    public static final long TORRENT_PEERS_DHT_UPDATE_TIMEOUT = 900_000;
    /**
     * period to send announce to trackers during download
     */
    public static final long TORRENT_PEERS_TRACKERS_UPDATE_TIMEOUT = 900_000;

    /**
     * ref to parent client that controls all the torrents,
     * dht node and other staff
     */
    oot.Client client;

    /**
     * metainfo of the torrent, parsed ".torrent" file
     */
    Metainfo metainfo;

    /**
     *  number of blocks in a piece, piece / 16K
     */
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
     * todo: each torrent is handled inside one thread except for save/load... review
     */
    private final Object piecesConfigurationLock = new Object();

    /**
     * state of pieces available on our size,
     * includes only pieces we have fully downloaded
     */
    final BitSet pieces;

    /**
     * timestamp of the last state save
     */
    long timeLastStateSave = 0;
    /**
     * timestamp of the last call to update
     */
    long timeLastUpdate = 0;
    /**
     * timestamp of the last torrent dump (debug only)
     */
    long timeLastDump = 0;
    /**
     * timestamp of the torrent initialization/(re)start
     */
    long timeTorrentStarted = 0;
    /**
     * timestamp of the torrent's finished event
     */
    long timeTorrentCompleted = 0;
    /**
     * timestamp of the last search for peers via DHT
     */
    long timeLastDHTUpdate = 0;

    /**
     * number of data bytes (as blocks) downloaded
     */
    AtomicLong downloaded = new AtomicLong();

    /**
     * number of data bytes (as blocks) uploaded
     */
    AtomicLong uploaded = new AtomicLong();



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

        /**
         * allowed constructor
         * @param blocks number of blocks in a piece
         */
        public PieceBlocks(int blocks) {
            ready = new BitSet(blocks);
            active = new BitSet(blocks);
            reset();
        }

        /**
         * resets to be reused
         */
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
    final Map<Integer, Torrent.PieceBlocks> piecesActive = new HashMap<>();
    /**
     * cache of instances to be reused and not recreated
     */
    private final ArrayDeque<Torrent.PieceBlocks> pieceBlocksCache = new ArrayDeque<>();

    /**
     * per-torrent storage api
     */
    private oot.Storage.TorrentStorage storage;

    /**
     * global state of this torrent
     */
    volatile Torrent.State state;

    /**
     * do we have all the data of this torrent or not,
     * must be based on pieces.cardinality()
     * todo: remove and check cardinality???
     */
    volatile boolean completed;

    /**
     * list of associated trackers
     */
    List<Tracker> trackers;

    /**
     * cache of PeerMessage instances...
     * they are short lived objects and could be handled by GC,
     * but this work well too
     */
    //PeerMessageCache pmCache = new PeerMessageCache();

    /**
     * download speed limit of this torrent, in bytes/sec
     */
    private long speedLimitDownload;
    /**
     * upload speed limit of this torrent, in bytes/sec
     */
    private long speedLimitUpload;

    /**
     * allowed constructor
     * @param _client client that handles torrents
     * @param _metainfo meta info of the torrent
     */
    private Torrent(Client _client, Metainfo _metainfo)
    {
        client = _client;
        metainfo = _metainfo;
        pieceBlocks = (int)(metainfo.pieceLength >> BLOCK_LENGTH_BITS);
        state = Torrent.State.NEW;
        pieces = new BitSet((int)metainfo.pieces);

        trackers = new ArrayList<>();
        for (int i = 0; i < metainfo.trackers.size(); i++) {
            List<String> urls = metainfo.trackers.get(i);
            Tracker tracker = new Tracker(this, urls);
            trackers.add(tracker);
        }
    }

    /**
     * allowed constructor
     * @param _client client that handles torrents
     * @param _metainfo meta info of the torrent
     * @param _storage storage to be used to read/write torrent data
     */
    public Torrent(Client _client, Metainfo _metainfo, Storage.TorrentStorage _storage) {
        this(_client, _metainfo);
        storage = _storage;
    }

    /**
     * @return ref to the associated client
     */
    public Client getClient() {
        return client;
    }

    public HashId getClientId() {
        return client.id;
    }

    /**
     * @return ref to the meta info of the torrent
     */
    public Metainfo getMetainfo() {
        return metainfo;
    }

    /**
     * @return torrent hash id
     */
    public HashId getTorrentId() {
        return metainfo.infohash;
    }

    /**
     * per-torrent selector, could be only one selector for a client now
     * @return selector
     */
    public Selector getSelector() {
        return getClient().selector;
    }

    /**
     * @return ref to the associated storage api
     */
    public Storage.TorrentStorage getStorage() {
        return storage;
    }

    /**
     * creates new instance of PieceBlocks to be used for
     * piece download, returns instance from the cache or
     * creates new one if cache is empty
     * @return not null instance ready to be used
     */
    private Torrent.PieceBlocks getPieceBlocksInstance()
    {
        Torrent.PieceBlocks pb;
        // todo: review and remove sync
        synchronized (pieceBlocksCache) {
            pb = pieceBlocksCache.pollFirst();
        }
        if (pb == null) {
            pb = new Torrent.PieceBlocks(pieceBlocks);
        } else {
            pb.reset();
        }
        return pb;
    }

    /**
     * returns used instance of PieceBlocks into cache to be used later
     * @param pb instance
     */
    private void releasePieceBlocks(Torrent.PieceBlocks pb) {
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
    private void markBlockDownloaded(int piece, int block)
    {
        synchronized (piecesConfigurationLock) {
            Torrent.PieceBlocks pb = piecesActive.get(piece);
            if (pb == null) {
                if (DEBUG) System.out.println("[MBLKD] p:" + piece + " b:" + block);
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
            Torrent.PieceBlocks pb = piecesActive.get(piece);
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
    private void markPieceDownloaded(int piece)
    {
        synchronized (piecesConfigurationLock) {
            Torrent.PieceBlocks pb = piecesActive.remove(piece);
            if (pb == null) {
                if (DEBUG) System.out.println("[MPD1]");
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
     * checks if the specified peer has pieces we are interested in,
     * this includes completely missing pieces and the ones being downloaded right now
     * @return true of there is at least one piece with data for us,
     */
    boolean interested(BitSet peerPieces) {
        long p = pieces.length();
        int i = -1;
        while ((i = pieces.nextClearBit(i + 1)) < p) {
            if (peerPieces.get(i)) {
                return true;
            }
        }
        return false;
    }


    /**
     * checks if the specified peer has pieces we are interested in
     * @param withoutActive active pieces will NOT be considered as interesting for us
     * @return true of there is at least one piece with data for us,
     */
    boolean interested(BitSet peerPieces, boolean withoutActive) {
        if (withoutActive) {
            return interested(peerPieces);
        }
        long p = pieces.length();
        int i = -1;
        while ((i = pieces.nextClearBit(i + 1)) < p) {
            if (peerPieces.get(i) && !piecesActive.containsKey(i)) {
                return true;
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
    int blocks(int piece) {
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
    boolean isLastBlock(int piece, int block) {
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
    int lastBlockSize() {
        return (int) (metainfo.length & BLOCK_LENGTH_MASK);
    }

    /**
     * called by a connection to provide more block requests,
     * number of requests controlled by connection itself within
     * the allowed throughput budget
     * @param pc connection that ask for new block requests
     * @param pPieces pieces available on the peer's side
     * @param requests number of request connection wants
     * @return number of requests allocated, could be less than requested at the end
     * of download process
     */
    int enqueueBlockRequests(PeerConnection pc, BitSet pPieces, int requests)
    {
        int allocated = enqueueBlocks(pc, pPieces, requests);
        while (allocated < requests) {
            int nextPiece = enqueuePiece(pPieces);
            if (nextPiece == -1) {
                // no more data we are interested in
                break;
            } else {
                allocated += enqueueBlocks(pc, pPieces, requests - allocated);
            }
        }
        return allocated;
    }

    /**
     * allocates specified number of blocks to be requested from a remote peer,
     * blocks are allocated in the way to be available for download
     * from the specified peer (be available on its side).
     * tries to allocate blocks in pieces that are being downloaded already (if available
     * on the specified peer) and allocates new piece if it's necessary and possible
     *
     * @param pc connection that is ready for new requests
     * @param pPieces pieces available on the peer's side
     * @param blocks number of blocks to allocate
     * @return number of blocks allocated and requested via the specified connection,
     * zero if there are no more blocks could be requested via the connection
     */
    private int enqueueBlocks(PeerConnection pc, BitSet pPieces, int blocks)
    {
        int allocated = 0;
        for (Map.Entry<Integer, Torrent.PieceBlocks> entry: piecesActive.entrySet())
        {
            int piece = entry.getKey();
            Torrent.PieceBlocks pb = entry.getValue();

            if (pieces.get(piece)) {
                // has been already downloaded, wtf?
                if (DEBUG) System.out.println("[ALBL-01]");
                continue;
            }

            if (!pPieces.get(piece)) {
                // peer doesn't have this piece yet,
                // can't request this from it
                continue;
            }

            // ok, try to allocate specific block inside this piece
            int allocatedInPiece = enqueueBlocksInsidePiece(pc, piece, pb, blocks);
            allocated += allocatedInPiece;
        }

        if (allocated < blocks) {
            // allocate next piece, but choose only that are available
            // at the specified peer
            int piece = enqueuePiece(pPieces);
            if (piece == -1) {
                // no more new pieces we can download from
                // this peer or pieces are over
                return allocated;
            } else {
                // allocate more blocks
                Torrent.PieceBlocks pb = piecesActive.get(piece);
                int allocatedInPiece = enqueueBlocksInsidePiece(pc, piece, pb, blocks);
                allocated += allocatedInPiece;
            }
        }

        return allocated;
    }

    /**
     * allocates specified number of blocks to be requested from a remote peer,
     * blocks are allocated inside the specified piece only.
     *
     * NOTE: called only from {@link #enqueueBlocks(PeerConnection, BitSet, int)} and
     * the piece requested is for sure exists on the peer's side
     *
     * @param pc connection that is ready for new requests
     * @param piece piece index to allocate blocks in
     * @param pb status object with information about the specified piece
     * @param amount number of blocks to allocate
     * @return number of blocks allocated and requested via the specified connection,
     * zero if there are no more blocks of the specified piece could be requested via the connection
     */
    private int enqueueBlocksInsidePiece(PeerConnection pc, int piece, Torrent.PieceBlocks pb, int amount)
    {
        BitSet ready = pb.ready;
        BitSet active = pb.active;
        int total = blocks(piece);

        int allocated = 0;
        int position = -1;
        while (true)
        {
            position = ready.nextClearBit(position + 1);
            if (total <=  position) {
                return allocated;
            }
            if (active.get(position)) {
                continue;
            }

            active.set(position);

            // inform connection to enqueue specific request it asked for
            int length = isLastBlock(piece, position) ? lastBlockSize() : BLOCK_LENGTH;
            pc.enqueueBlockRequest(piece, position << BLOCK_LENGTH_BITS, length);

            allocated += 1;
            if (allocated == amount) {
                return allocated;
            }
        }
    }

    /**
     * allocates new piece to be downloaded by all connections
     * NOTE: called only from {@link #enqueueBlocks(PeerConnection, BitSet, int)}
     *
     * @param pPieces pieces available on the side of the peer, that requested more blocks to download
     * @return index of the piece allocated or -1 if no more new pieces available,
     * that means last pieces are being downloaded already or the peer has no more pieces
     */
    private int enqueuePiece(BitSet pPieces)
    {
        int index = -1;

        // todo: allocate random piece ?
        while (true)
        {
            index = pieces.nextClearBit(index + 1);

            if (metainfo.pieces <=  index) {
                return -1;
            }
            if (piecesActive.containsKey(index)) {
                continue;
            }
            if (!pPieces.get(index)) {
                continue;
            }

            // prepare state object and make piece active
            Torrent.PieceBlocks pb = getPieceBlocksInstance();
            piecesActive.put(index, pb);

            return index;
        }
    }

    /**
     * tries to find new peers for this torrent via DHT if available
     * @param now timestamp
     */
    void getMorePeersFromDht(long now)
    {
        if (client.isDhtEnabled()) {
            if (timeLastDHTUpdate + TORRENT_PEERS_DHT_UPDATE_TIMEOUT < now)
            {
                timeLastDHTUpdate = now;
                client.node.findPeers(metainfo.infohash, this::addPeersFromAddresses);
            }
        }
    }


    /**
     * this method is called periodically by client to update state,
     * open/close new connections, send keep alive messages,
     * perform some maintenance, etc.
     */
    void update()
    {
        long now = System.currentTimeMillis();
        timeLastUpdate = now;

        if (state == Torrent.State.NEW)
        {
            timeTorrentStarted = now;

            client.trackersManager.announce(this, TrackersManager.AnnounceEvent.STARTED);
            getMorePeersFromDht(now);

            state = Torrent.State.INITIALIZING;
            restoreState(b -> {
                if (b) {
                    state = Torrent.State.ACTIVE;
                } else {
                    storage.init(result -> {
                        if (!result) {
                            state = Torrent.State.ERROR;
                        } else {
                            state = Torrent.State.ACTIVE;
                        }
                    });
                }
            });
        }

        if (state == Torrent.State.ACTIVE)
        {
            updateConnections();

            getMorePeersFromDht(now);

            // todo re-announce?

            if (!completed && (TORRENT_STATE_SAVE_TIMEOUT < timeLastUpdate - timeLastStateSave)) {
                saveState();
                timeLastStateSave = timeLastUpdate;
            }
        }

        if (state == Torrent.State.STOPPING) {
            // ?timeout?
            boolean stopped = true;
            for (Map.Entry<Peer, PeerConnection> entry: connections.entrySet()) {
                PeerConnection pc = entry.getValue();
                if (pc.getActiveBlockRequestsNumber() <= 0) {
                    pc.close(Peer.CloseReason.NORMAL);
                } else {
                    stopped = false;
                    break;
                }
            }
            if (stopped) {
                // force close & remove connections
                connections.values().forEach(pc -> pc.close(Peer.CloseReason.NORMAL));
                connections.keySet().removeIf(Peer::isConnectionClosed);

                // notify trackers
                client.trackersManager.announce(this, TrackersManager.AnnounceEvent.STOPPED);

                state = Torrent.State.STOPPED;
            }
        }

        // merge peers collections
        moveNewPeersToMainCollection();
    }


    /**
     * performs logic linked to all connections..
     */
    private void updateConnections()
    {
        long now = System.currentTimeMillis();

        // remove connections that were not able to finish
        // connecting phase or were disconnected while
        // processing peer2peer communication
        connections.keySet().removeIf(Peer::isConnectionClosed);

        // todo: use speed limits here to limit connections

        // number of connections we may open
        int toOpen = 0;
        if (completed)
        {
            if ((CONNECTIONS_SEED_MAX < connections.size())
                    && (timeTorrentCompleted + CONNECTIONS_SEED_DROP_TIMEOUT < now))
            {
                // we have completed torrent here and too many active connections,
                // seed-seed connections should be already dropped via updateConnection()
                // due to CONNECTIONS_SEED_DROP_TIMEOUT timeout
                closeConnections(CONNECTIONS_SEED_MAX);
            }
            // some connection could have finished downloading from us,
            // try to open connections to another peers (private ip mode)
            toOpen = CONNECTIONS_SEED_MAX - connections.size();
        } else {
            toOpen = CONNECTIONS_DOWNLOAD_MAX - connections.size();
        }

        while (0 < toOpen--) {
            openConnection();
        }

        // let each connection to update itself - connect, etc.
        connections.forEach((peer, pc) -> pc.update());
    }

    /**
     * closes extra connections choosing ones with the slowest upload rate
     * @param allowed number of connections to leave be
     */
    private void closeConnections(int allowed)
    {
        // force close seed-seed connections (must be closed already),
        // [could check interested/peerInterested flags]
        connections.entrySet().removeIf(entry -> {
            PeerConnection pc = entry.getValue();
            boolean interesting = interested(pc);
            if (!interesting) {
                if (DEBUG) System.out.println(pc.peer.address + " forced close of s2s connection");
                pc.close(Peer.CloseReason.NORMAL);
                return true;
            }
            return false;
        });

        if (connections.size() <= allowed) {
            return;
        }

        // make stable copy of connections and remove the slowest ones
        PeerConnection[] pcs = connections.values().toArray(PeerConnection[]::new);
        long[] speeds = Arrays.stream(pcs).mapToLong(pc -> pc.getUploadSpeed(2)).toArray();
        long[] sorted = Arrays.stream(speeds).sorted().toArray();
        long bound = sorted[sorted.length - allowed - 1];
        for (int i = 0; i < pcs.length; i++) {
            PeerConnection pc = pcs[i];
            if (speeds[i] <= bound) {
                pc.close(Peer.CloseReason.NORMAL);
                connections.remove(pc.peer);
            }
        }
    }


    /**
     * finds another peer in the list of available ones
     * and tries to open connection to it,
     * on success registers new connection in {@link Torrent#connections}
     */
    private void openConnection()
    {
        for (Peer peer : peers) {
            if (connections.containsKey(peer)) {
                // already connected or connecting
                continue;
            }
            if (!peer.isConnectionAllowed()) {
                // still waiting for reconnect to be allowed
                continue;
            }
            if (completed && peer.isCompleted()) {
                // seed mode and peer already has all the pieces,
                // nobody needs this connection
                continue;
            }

            // remove error state as it's possible we are going to reconnect
            peer.resetConnectionClosed();

            // todo: think about upgrading connection to specific protocol??
            PeerConnection pc = new StdPeerConnection(this, peer, );
            connections.put(peer, pc);
            setDownloadSpeedLimit(speedLimitDownload);
            break;
        }
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
     * external api method to add new peers to the torrent
     * @param newPeers collection of peers' addresses
     */
    public void addPeersFromAddresses(Collection<InetSocketAddress> newPeers) {
        if (newPeers == null) {
            return;
        }
        synchronized (peersSyncAdd) {
            for (InetSocketAddress isa: newPeers) {
                Peer peer = new Peer(isa);
                peersSyncAdd.add(peer);
            }
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


    /**
     * called from this torrent when last
     * piece of torrent is finished
     */
    void onFinished()
    {
        completed = true;
        timeTorrentCompleted = System.currentTimeMillis();
        saveState();

        if (DEBUG) {
            long time = timeTorrentCompleted - timeTorrentStarted;
            System.out.println(String.format("finished downloading: %s  time: %d sec",  metainfo.infohash.toString(), time/1000));
        }
    }


    /**
     * called by a connection when correct block of data received from remote side
     * @param pc connection that has received the data
     * @param buffer buffer with (pos, limit) set to point to data
     * @param index piece index
     * @param begin block position inside the piece
     * @param length length of the block, must be == buffer.remaining()
     */
    void onPiece(PeerConnection pc, ByteBuffer buffer, int index, int begin, int length)
    {
        if (DEBUG) System.out.println("onPiece: " + index + "  " + (begin >> 14));

        storage.writeBlock(buffer, index, begin, length, (result) -> {
            // this could be called from some other thread (storage)
            int block = begin >> BLOCK_LENGTH_BITS;
            markBlockDownloaded(index, block);
            downloaded.addAndGet(Torrent.BLOCK_LENGTH);
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
        // buffer will be released in send() on message serialization
        final ByteBuffer buffer = storage.getBuffer();
        storage.readBlock(buffer, index, begin, length, result -> {
            StdPeerMessage pm = pmCache.piece(index, begin, length, buffer);
            pc.enqueue(pm);
            uploaded.addAndGet(Torrent.BLOCK_LENGTH);
        });
    }

    /**
     * called from a connection to notify that physical connection is closed
     * @param pc peer connection
     */
    void onPeerDisconnect(PeerConnection pc) {
        // peer will be removed in update()
        //System.out.println(pc.peer.address + " error / disconnected");
        //new Exception().printStackTrace();
    }

    /**
     * populates given structures with copy of the state of the torrent
     * @param _pieces state of all pieces
     * @param _active state of the pieces being downloaded
     */
    public void getCompletionState(BitSet _pieces, Map<Integer, BitSet> _active)
    {
        _pieces.clear();
        _pieces.or(pieces);

        _active.clear();
        piecesActive.forEach( (p, m) -> {
            BitSet tmp = new BitSet(pieceBlocks);
            tmp.or(m.ready);
            _active.put(p, tmp);
        });
    }

    /**
     * initiates save store via the associated storage api
     */
    private void saveState() {
        storage.writeState(pieces, piecesActive);
    }

    /**
     * restores state loading it from the storage
     * @param callback callback to be notified with true if state was successfully restored and
     *                 false if it's missing or there were some errors
     */
    private void restoreState(Consumer<Boolean> callback) {
        storage.readState(pieces, piecesActive, callback);
        completed = pieces.cardinality() == metainfo.pieces;
    }



    /**
     * note: not real history, only active connections are used for calculation
     * @param seconds number of seconds
     * @return average download speed for all connections of this torrent
     */
    public long getDownloadSpeed(int seconds) {
        long total = 0;
        for (PeerConnection pc : connections.values()) {
            total += pc.getDownloadSpeed(seconds);
        }
        return total;
    }

    /**
     * note: not real history, only active connections are used for calculation
     * @param seconds number of seconds
     * @return average upload speed for all connections of this torrent
     */
    public long getUploadSpeed(int seconds) {
        long total = 0;
        for (PeerConnection pc : connections.values()) {
            total += pc.getUploadSpeed(seconds);
        }
        return total;
    }

    /**
     * sets download limit for this torrent, managed by parent client,
     * note: could be easily extended to support per torrent limits
     * @param budget number of bytes/sec
     */
    public void setDownloadSpeedLimit(long budget)
    {
        speedLimitDownload = budget;

        if (connections.size() == 0) {
            return;
        }

        // reset connections' limit if reset fot his torrent
        if (speedLimitDownload == 0) {
            for (PeerConnection pc : connections.values()) {
                pc.speedLimitDownload = 0;
            }
            return;
        }

        // make stable copy
        PeerConnection[] pcs = connections.values().toArray(PeerConnection[]::new);

        // calculate download speeds of all connections
        long[] speeds = new long[pcs.length];
        long totalSpeed = 0;
        for (int i = 0; i < pcs.length; i++) {
            PeerConnection pc = pcs[i];
            long speed = pc.getDownloadSpeed(2);
            speeds[i] = speed;
            totalSpeed += speed;
        }

        long averageSpeedLimit = speedLimitDownload / pcs.length;
        long availableSpeedBudget = speedLimitDownload - totalSpeed;

        int index = 0;
        int hightSpeedCount = 0;
        for (PeerConnection pc : pcs) {
            long speed = speeds[index++];

            if (speed < averageSpeedLimit - Torrent.BLOCK_LENGTH) {
                // let this torrent to use more bandwidth,
                // soft target is the same speed for all
                pc.speedLimitDownload = averageSpeedLimit;
            } else {
                // plan this connection for more processing
                hightSpeedCount++;
            }
        }

        // divide not used throughput equally between quick connections
        if (0 < hightSpeedCount) {
            index = 0;
            long averageSpeedBudget = availableSpeedBudget / hightSpeedCount;
            for (PeerConnection pc : pcs) {
                long speed = speeds[index++];
                if (averageSpeedLimit - Torrent.BLOCK_LENGTH <= speed) {
                    pc.speedLimitDownload = speed + averageSpeedBudget;
                }
            }
        }
    }


    /**
     * dumps active connections of the torrent to stdout
     */
    public void dump()
    {
        Formatter formatter = new Formatter();
        formatter.format("                              L  P                                 \n");
        formatter.format("                          C H CI CI   DLR  RQ   BLKS |   UPL  Q   BLKS\n");

        connections.values().forEach(pc -> pc.dump(formatter));

//        formatter.format(" peer messages created: %d\n", PeerMessageCache.counter);
//        formatter.format("     buffers allocated: %d\n", SimpleFileStorage.buffersAllocated);
        formatter.format("                 state: %s\n", state.name());
        formatter.format("            completion: %.2f\n", 100.0 * pieces.cardinality() / metainfo.pieces);
        formatter.format("                 peers: %d\n", peers.size());
        //formatter.format("       save task queue: %d\n", SimpleFileStorage.exSave.getQueue().size());

        System.out.println(formatter.toString());
    }
}
