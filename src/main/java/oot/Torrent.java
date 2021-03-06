package oot;

import oot.be.Metainfo;
import oot.dht.HashId;
import oot.storage.TorrentStorage;
import oot.tracker.*;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

public class Torrent
{
    // debug switch
    private static final boolean DEBUG = false;

    /**
     * lock used by runner threads to run torrents
     * in bound of one thread,
     * could be moved to a separate collection
     * in client
     */
    final ReentrantLock runnerLock = new ReentrantLock();

    /**
     * internal states of the torrent
     */
    enum State {
        /**
         * new torrent, usually after creation of a new torrent,
         * must to load state, checks files, bind file channels, etc.
         */
        NEW,
        /**
         * torrent is active and serves connections,
         * could be in seed/download modes,
         * behavior is controlled by a strategy
         */
        ACTIVE,
        /**
         * torrent is stopped and doesn't perform any upload/download,
         * drops outgoing requests, doesn't accept new peer requests,
         * then closes all connections,
         * may move storage to unknown state
         */
        STOPPED,
        /**
         * something is wrong
         */
        ERROR,
        /**
         * dummy state used to wait for some other action to finish
         * and set the state,
         * todo: usually controlled with a timeout. moving to ERROR state after it
         */
        WAIT
    }

    /**
     * describes status of a piece dividing it into blocks
     * downloaded separately from (possibly) different connections
     */
    public static class PieceBlocks implements Serializable, Cloneable {
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

        @Override
        public Object clone() throws CloneNotSupportedException {
            PieceBlocks clone = (PieceBlocks) super.clone();
            clone.ready = (BitSet) ready.clone();
            clone.active = (BitSet) active.clone();
            return clone;
        }
    }

    /**
     * connections factory to be used to open new connections
     * todo: move to strategy
     */
    PeerConnectionFactory pcFactory;


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
     * period to send announce to trackers during download
     */
    public static final long TORRENT_PEERS_TRACKERS_UPDATE_TIMEOUT = 900_000;

    /**
     * ref to id of the parent client that controls all the torrents
     */
    //final HashId clientId;
    /**
     * ref to the controlling client
     */
    final Client client;

    /**
     * metainfo of the torrent, parsed ".torrent" file
     */
    public final Metainfo metainfo;

    /**
     *  number of blocks in a piece, piece / 16K
     */
    final int pieceBlocks;

    /**
     * list of all known peers, possibly not accessible,
     * populated from torrent metainfo, DHT, peer exchange, etc.
     */
    Set<Peer> peers = new HashSet<>();
    /**
     * utility collection used to add new peers into the main
     * peers collection from other threads
     */
    final Set<Peer> peersSyncAdd = new HashSet<>();

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
    //private final Object piecesConfigurationLock = new Object();

    /**
     * state of pieces available on our size,
     * includes only pieces we have fully downloaded
     */
    BitSet pieces;

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
     * number of data bytes (as blocks) downloaded,
     * exposed to external clients
     */
    public AtomicLong downloaded = new AtomicLong();

    /**
     * number of data bytes (as blocks) uploaded,
     * exposed to external clients
     */
    public AtomicLong uploaded = new AtomicLong();

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
    TorrentStorage storage;

    /**
     * global state of this torrent
     */
    Torrent.State state;

    /**
     * do we have all the data of this torrent or not,
     * must be based on pieces.cardinality()
     */
    boolean completed;
    /**
     * == completed
     * mem barrier after data download?
     *
     * todo: must check on new new torrent/load state/files check
     *
     * todo: 2 flags is the overkill
     */
    volatile boolean finished;

    /**
     * list of associated trackers to announce
     */
    List<Tracker> trackers = new ArrayList<>();

    /**
     * download speed limit of this torrent, in bytes/sec
     */
    private long speedLimitDownload;
    /**
     * upload speed limit of this torrent, in bytes/sec
     */
    private long speedLimitUpload;


    /**
     * service queue with parameters of blocks written to storage,
     * used to separate main processing thread and callback running in a storage thread
     */
    protected final ArrayDeque<TorrentStorage.Block> written = new ArrayDeque<>(256);

    /**
     * service queue with parameters of blocks read from storage,
     * used to separate main processing thread and callback running in a storage thread
     */
    protected final ArrayDeque<TorrentStorage.Block> read = new ArrayDeque<>(256);

    /**
     * service queue to run commands/callback inside the processing thread,
     * commands are triggered by {@link #update()} call from a thread runner class,
     * mostly should be used for commands without strong latency requirements
     */
    protected final ArrayDeque<TorrentCommand> commands = new ArrayDeque<>(256);

    /**
     * public api to work with the torrent,
     * could be used internally as a convenient choice
     */
    protected final Api api = new Api();

    /**
     * allowed constructor
     * @param _client client id that handles torrents
     * @param _metainfo meta info of the torrent
     * @param _pcFactory factory to open new connections
     */
    protected Torrent(Client _client, Metainfo _metainfo, PeerConnectionFactory _pcFactory)
    {
        client = _client;
        metainfo = _metainfo;
        pcFactory = _pcFactory;

        pieceBlocks = (int)(metainfo.pieceLength >> BLOCK_LENGTH_BITS);
        pieces = new BitSet((int)metainfo.pieces);

        setState(Torrent.State.NEW);
    }

    /**
     * allowed constructor
     * @param _client client that handles torrents
     * @param _metainfo meta info of the torrent
     * @param _storage storage to be used to read/write torrent data
     */
    protected Torrent(Client _client, Metainfo _metainfo, PeerConnectionFactory _pcFactory, TorrentStorage _storage) {
        this(_client, _metainfo, _pcFactory);
        storage = _storage;
    }


    @Override
    public int hashCode() {
        return metainfo.infohash.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Torrent torrent) {
            return metainfo.infohash.equals(torrent.metainfo.infohash);
        }
        return false;
    }

    /**
     * sets new state of this torrent
     * @param _state new state
     */
    protected void setState(State _state) {
        state = _state;
        // notify client about state change
        client.torrentStateChanged(this, _state);
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
     * @return ref to the associated storage api
     */
    public TorrentStorage getStorage() {
        return storage;
    }

    /**
     * @return readiness state of the torrent
     */
    public boolean isFinished() {
        return finished;
    }

    /**
     * creates new instance of PieceBlocks to be used for
     * piece download, returns instance from the cache or
     * creates new one if cache is empty
     * @return not null instance ready to be used
     */
    private Torrent.PieceBlocks getPieceBlocksInstance()
    {
        Torrent.PieceBlocks pb = pieceBlocksCache.pollFirst();
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
    private void releasePieceBlocks(Torrent.PieceBlocks pb)
    {
        pieceBlocksCache.offerFirst(pb);
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

    /**
     * removes block from the list of active (being downloaded) ones
     * to be queried later
     * @param piece piece index
     * @param block block index
     */
    private void markBlockCancelled(int piece, int block)
    {
        System.out.println("[torrent] markBC: " + piece + "  " + block);

        Torrent.PieceBlocks pb = piecesActive.get(piece);
        if (pb != null) {
            System.out.println("-X- " + piece + "  " + block + "   E:  r:" + pb.ready.get(block) + " a:" + pb.active.get(block));
            pb.active.clear(block);
        } else {
            System.out.println("[MBLKC]");
            new Exception().printStackTrace();
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


    /**
     * checks if the specified peer has pieces we are interested in,
     * checks only against fully downloaded pieces, active pieces
     * are considered as interesting
     * NOTE: this could return "interested", but all missing requests
     *  are being processed by some connection(s), so next call
     *  to enqueue will do nothing (that's ok)
     *
     * @return true of there is at least one piece with data for us,
     */
    boolean interested(BitSet peerPieces)
    {
        // can't use pieces.length() as it depends on data
        long p = metainfo.pieces;
        int i = -1;
        while ((i = pieces.nextClearBit(i + 1)) < p) {
            if (peerPieces.get(i)) {
                return true;
            }
        }
        return false;
    }


    /**
     * checks if the specified peer has pieces we are interested in,
     * checks against fully and partially downloaded pieces.
     * partially downloaded considered as NOT interesting
     * NOTE: this could lead to errors, for example:
     *  pieces [0,1] - downloaded
     *  pieces [2,3] - active, but only 1 block is requested from #3
     *  so we are "not interested" and could miss (not send) block request for #3
     *
     * @param withoutActive if true works as {@link #interested(BitSet)}
     * @return true of there is at least one piece with data for us,
     */
    boolean interested(BitSet peerPieces, boolean withoutActive)
    {
        if (withoutActive) {
            return interested(peerPieces);
        }
        // can't use pieces.length() as it depends on data
        long p = metainfo.pieces;
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
            return (piece < metainfo.pieces - 1) ? pieceBlocks : 0;
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
    protected int enqueueBlocks(PeerConnection pc, BitSet pPieces, int blocks)
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
    protected int enqueueBlocksInsidePiece(PeerConnection pc, int piece, Torrent.PieceBlocks pb, int amount)
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
    protected int enqueuePiece(BitSet pPieces)
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
     * performs logic linked to all connections..
     */
    protected void updateConnections()
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
            openConnection();         // todo: <-- add check for impossible as returned int
        }

        // let each connection to update itself - connect, etc.
        connections.forEach((peer, pc) -> pc.update());
    }

    /**
     * closes extra connections choosing ones with the slowest upload rate
     * @param allowed number of connections to leave be
     */
    protected void closeConnections(int allowed)
    {
        // force close seed-seed connections (must be closed already),
        // [could check interested/peerInterested flags]
        connections.entrySet().removeIf(entry -> {
            PeerConnection pc = entry.getValue();
            boolean interesting = interested(pc.getPeerPieces());
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
    protected void openConnection()
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

            // todo: strategy, think about upgrading connection to specific protocol??
            PeerConnection pc = pcFactory.openConnection(peer, this);
            connections.put(peer, pc);
            setDownloadSpeedLimit(speedLimitDownload);
            break;
        }
    }


    /**
     * internal method to be called on the thread that processes the torrent,
     * moves all new peer from peersSyncAdd collection to the main one,
     * should be called on state update
     */
    protected void moveNewPeersToMainCollection() {
        synchronized (peersSyncAdd) {
            peers.addAll(peersSyncAdd);
            peersSyncAdd.clear();
        }
    }

    /**
     * called when last
     * piece of torrent has been downloaded
     */
    protected void onFinished()
    {
        completed = true;
        timeTorrentCompleted = System.currentTimeMillis();
        writeState();

        // this could be used to read state?
        finished = true;

        if (DEBUG) {
            long time = timeTorrentCompleted - timeTorrentStarted;
            System.out.println(String.format("finished downloading: %s  time: %d sec",  metainfo.infohash.toString(), time/1000));
        }
    }


    /**
     * called by a connection when correct block of data received from remote side.
     * NOTE: ref to buffer MUST NOT be saved inside,
     * data must be copied in case of async processing
     *
     * @param pc connection that has received the data
     * @param buffer buffer with (pos, limit) set to point to data
     * @param index piece index
     * @param begin block position inside the piece
     * @param length length of the block, must be == buffer.remaining()
     */
    protected void onPiece(PeerConnection pc, ByteBuffer buffer, int index, int begin, int length)
    {
        if (DEBUG) System.out.println(System.nanoTime() + "  [onPiece] " + index + "  " + (begin >> 14) + "      " + begin + "," + length);

        System.out.println(System.nanoTime() + "  [onPiece] " + index + "  " + (begin >> 14) + "      " + begin + "," + length);

        storage.write(buffer, index, begin, length, (block) -> {
            if (block == null) {
                // todo: new instance of lambda is possible here, --> make inner class ?
                if (DEBUG) System.out.println("torrent.onPiece: null block received");
            } else {
                synchronized (written) {
                    written.offer(block);
                }
            }
        });

    }

    /**
     * called by the main processing thread to mark
     * saved blocks (stored on a separate queue) as downloaded
     */
    protected void onPieceProcessWrittenBlocks()
    {
        synchronized (written) {
            TorrentStorage.Block block;
            while ((block = written.poll()) != null) {
                // todo: check parameters ?
                markBlockDownloaded(block.index, block.position >> BLOCK_LENGTH_BITS);
                downloaded.addAndGet(Torrent.BLOCK_LENGTH);
                // todo: send HAVE to all open connections (which doesn't have it?)
            }
        }
    }

    /**
     * called by a connection when correct request for a block of data received from remote side
     * @param pc connection that has received the data
     * @param index piece index
     * @param begin block position inside the piece
     * @param length length of the block, must be == buffer.remaining()
     */
    protected void onRequest(PeerConnection pc, int index, int begin, int length)
    {
        storage.read(index, begin, length, pc, block -> {
            synchronized (read) {
                read.offer(block);
            }
        });
    }

    /**
     * called by the main processing thread to enqueue
     * loaded blocks (stored on a separate queue) to the connection
     */
    protected void onRequestProcessReadBlocks()
    {
        synchronized (read) {
            TorrentStorage.Block block;
            while ((block = read.poll()) != null) {
                PeerConnection pc = (PeerConnection)block.param;

                if (connections.containsKey(pc.peer)) {
                    // connection will need to call release() on the block
                    pc.enqueuePiece(block.buffer, block.index, block.position, block.length, block);

                    System.out.println("[torrent] onRPRB " + block.index +","+ block.position +","+ block.length +
                            "  [0]:" + block.buffer.get(0) + "   " + block.buffer);
                    uploaded.addAndGet(block.length);
                } else {
                    // forget about block
                    block.release();
                }
            }
        }
    }

    /**
     * called by active @{@link PeerConnection} to get possible ready to be sent messages,
     * that could be asynchronously finished read requests (PIECE, REQUEST) that must
     * be delivered to connections' queues
     *
     * @param pc peer connection that performs the call
     */
    protected void onConnectionReadyToSend(PeerConnection pc)
    {
        // process async responses from the storage,
        // this marks downloaded and stored blocks
        onPieceProcessWrittenBlocks();
        // this enqueues pieces received from the storage
        // to be send over a connection (also called before send)
        onRequestProcessReadBlocks();
    }

    /**
     * called from a connection to notify that physical connection is closed
     * @param pc peer connection
     */
    protected void onPeerDisconnect(PeerConnection pc) {
        // peer will be removed from connections list in updateConnections()
        // but release resources now
        pcFactory.closeConnection(pc);
    }

    /**
     * populates given structures with copy of the state of the torrent
     * @param _pieces state of all pieces
     * @param _active state of the pieces being downloaded
     */
    protected void getCompletionState(BitSet _pieces, Map<Integer, BitSet> _active)
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
     * initiates save store via the associated storage api,
     * must be called only from a runner thread or via cmd
     */
    protected void writeState()
    {
        BitSet piecesClone = (BitSet) pieces.clone();
        Map<Integer, Torrent.PieceBlocks> piecesActiveClone = new HashMap<>(piecesActive);
        storage.writeState(piecesClone, piecesActiveClone);
    }

    /**
     * restores state loading it from the storage,
     *
     * @param callback callback to be notified with true if state was successfully restored and
     *                 false if it's missing or there were some errors
     */
    protected void readState(Consumer<Boolean> callback)
    {
        BitSet piecesClone = new BitSet(pieces.size());
        Map<Integer, Torrent.PieceBlocks> piecesActiveClone = new HashMap<>();

        storage.readState(piecesClone, piecesActiveClone, result -> {
            // this could be called on storage thread
            addCommand((torrent) -> {
                // this is called in runner thread
                if (result) {
                    pieces.clear();
                    pieces.or(piecesClone);
                    piecesActive.clear();
                    piecesActive.putAll(piecesActiveClone);

                    completed = pieces.cardinality() == metainfo.pieces;
                    finished = completed;
                }
                callback.accept(result);
            });
        });
    }

    /**
     * processes commands that must be called
     * in torrent's runner thread
     */
    protected void processCommandQueue()
    {
        TorrentCommand fun;
        do {
            synchronized (commands) {
                fun = commands.poll();
            }
            if (fun != null) {
                fun.execute(this);
            }
        } while (fun != null);
    }

    /**
     * adds command to be executed in torrent's
     * runner thread, commands will be executed within {@link #update()} method,
     *
     * could be called directly but mostly supposed to be used via {@link TorrentCommands}
     * @param fun function to run
     */
    public void addCommand(TorrentCommand fun) {
        synchronized (commands) {
            commands.offer(fun);
        }
    }

    /**
     * executes "starts" command received by this torrent,
     * must be called only from a runner thread via sending
     * command to this torrent via public api
     */
    protected void start()
    {
        State _state = state;

        if (_state == State.ACTIVE) {
            // already started
            return;
        }

        ////////////////////////////////////////////////////////////////////
        if ((_state == State.NEW) || (_state == State.STOPPED))
        {
            TorrentStorage.State tsState = storage.getState();
            if (tsState == TorrentStorage.State.UNKNOWN) {
                // ok, check if we have any state for the torrent
                readState(success ->
                {
                    // this will run in torrent thread,
                    // due to method contract this will happen only once
                    // as storage mode will be changes on exit
                    storage.bind(!success, true, false, null);
                });
                // ACTIVE will wait for storage to be BOUND|ERROR
                setState(State.ACTIVE);
            }
            else if (tsState == TorrentStorage.State.ERROR) {
                // that is strange, new torrent with incorrect storage,
                // switch to error state
                setState(State.ERROR);
            }
            else if (tsState == TorrentStorage.State.BOUND) {
                // switch to ACTIVE state, it will handle
                // possible storage errors
                setState(State.ACTIVE);
            }
            else {
                // all other storage states are intermediate,
                // we are not interested in them... try to run in ACTIVE,
                // it will handle errors
                setState(State.ACTIVE);
            }

            return;
        }

        ////////////////////////////////////////////////////////////////////
        if (_state == State.ERROR)
        {
            TorrentStorage.State tsState = storage.getState();
            if (tsState == TorrentStorage.State.BOUND) {
                // strange, just try to run
                setState(State.ACTIVE);
            }
            else if (tsState == TorrentStorage.State.UNKNOWN)
            {
                // that should not happen, handle as NEW/UNKNOWN
                readState(success ->
                {
                    // this will run in torrent thread,
                    // due to method contract this will happen only once
                    // as storage mode will be changes on exit
                    storage.bind(!success, true, false, null);
                });
                // try to run it
                setState(State.ACTIVE);
            }
            else if (tsState == TorrentStorage.State.ERROR)
            {
                TorrentStorage.ErrorDetails eDetails = storage.getErrorStateDetails();
                boolean recheck = (eDetails != null) && (eDetails.type == TorrentStorage.ErrorType.INTEGRITY);
                storage.bind(false, false, true, (sState, bitSet) -> {
                    if (sState == TorrentStorage.State.BOUND) {
                        pieces.clear();
                        pieces.or(bitSet);
                    }
                });

                // update() will wait for storage reply
                setState(State.ACTIVE);
            }
            return;
        }
    }

    protected void stop() {

    }

    /**
     * this method is called periodically by client to update state,
     * open/close new connections, send keep alive messages,
     * perform some maintenance, etc.
     */
    protected void update()
    {
        timeLastUpdate = System.currentTimeMillis();

        // todo: move to states ?
        // todo: call after receive/send

        // process async responses from the storage,
        // this marks downloaded and stored blocks
        onPieceProcessWrittenBlocks();
        // this enqueues pieces received from the storage
        // to be send over a connection (also called before send)
        onRequestProcessReadBlocks();

        // process externally submitted commands that
        // must be executed in runner thread
        processCommandQueue();

        // merge peers collections
        moveNewPeersToMainCollection();

        // state specific logic
        switch (state) {
            case NEW -> updateInNewState();
            case ACTIVE -> updateInActiveState();
            case STOPPED -> updateInStoppedState();
            default -> {}
        }

    }

    protected void updateInNewState()
    {
        // do nothing for now
    }

    protected void updateInActiveState()
    {
        TorrentStorage.State tsState = storage.getState();
        if (tsState == TorrentStorage.State.ERROR) {
            // something went wrong during the processing,
            // stop torrent
            setState(State.ERROR);
            return;
        }

        if (tsState != TorrentStorage.State.BOUND) {
            // wait till storage is ready,
            // must not often happen here, just another protection,
            // nut could be used when switching to this state
            return;
        }

        // main logic is there, it handles seed/download modes
        updateConnections();

        // try to get more peers, this checks timeout inside for each tracker
        trackers.forEach(tracker -> tracker.updateIfReady( (success, ps) -> {
            if (success) {
                api.addPeersFromAddresses(ps);
            }
        }));

        // todo re-announce?

        if (!completed && (TORRENT_STATE_SAVE_TIMEOUT < timeLastUpdate - timeLastStateSave)) {
            writeState();
            timeLastStateSave = timeLastUpdate;
        }
    }

    protected void updateInStoppedState()
    {
        // ?timeout?
        boolean stopped = true;
        for (Map.Entry<Peer, PeerConnection> entry: connections.entrySet()) {
            PeerConnection pc = entry.getValue();
            if (!pc.isDownloading()) {
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
            trackers.forEach( Tracker::stopped );

            setState(Torrent.State.STOPPED);
        }
    }

    /**
     * public API to work with a torrent
     */
    public class Api {
        /**
         * schedules "torrent::start" command to be run in a runner thread
         * @return future that will be completed on operation finish,
         * it will be completed with "true" only if torrent has been switched to ACTIVE state,
         * but it could have been moved to some intermediate state and will switch to ACTIVE later
         */
        public CompletableFuture<Boolean> start()
        {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            addCommand(torrent -> {
                torrent.start();
                future.complete(torrent.state == State.ACTIVE);
            });
            return future;
        }

        /**
         * schedules "torrent::stop" command to be run in a runner thread
         * @return future that will be completed on operation finish
         */
        public CompletableFuture<Void> stop()
        {
            CompletableFuture<Void> future = new CompletableFuture<>();
            addCommand(torrent -> {
                torrent.stop();
                future.complete(null);
            });
            return future;
        }


        /**
         * external api method to add new peers to the torrent
         * @param addresses collection of peers' addresses
         */
        public void addPeersFromAddresses(Collection<InetSocketAddress> addresses)
        {
            if (addresses == null) {
                return;
            }
            synchronized (peersSyncAdd) {
                for (InetSocketAddress isa: addresses) {
                    Peer peer = new Peer(isa);
                    peersSyncAdd.add(peer);
                }
            }
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
         * allows a function to process list of registered trackers inside the torrent
         * @param func function to be called inside processing thread
         * @return result of the function
         */
        public <R> CompletableFuture<R> trackers(Function<List<Tracker>, R> func)
        {
            CompletableFuture<R> future = new CompletableFuture<>();
            addCommand(torrent -> {
                R result = func.apply(trackers);
                future.complete(result);
            });
            return future;
        }

        /**
         * adds another tracker to the list of torrent's trackers
         * @param tracker new tracker
         * @return future that will be completed on operation finish
         */
        public CompletableFuture<Void> addTracker(Tracker tracker)
        {
            CompletableFuture<Void> future = new CompletableFuture<>();
            addCommand(torrent -> {
                torrent.trackers.add(tracker);
                future.complete(null);
            });
            return future;
        }


        /**
         * adds another connection to the torrent's list of active connections,
         * this is more like a service function, but could be used to dev/debug
         * new connection types
         * @param pc connection
         */
        public void addConnection(PeerConnection pc)
        {
            addCommand((x) -> connections.put(pc.peer, pc));
        }


        void getPieces() {}
        void setPieces() {}
        void setPieces(int idx) {}

        void getState() {}
        void setState() {}

        void getStorageApi() {}
        void getSState() {}
        void getInputStream() {}
        void getOutputStream() {}
        void bind() {}

        /**
         * creates "torrent::writeState" command for the given torrent
         * and places it into the commands' queue of the torrent
         * to be executed later
         * @param torrent torrent
         */
        public void cmdTorrentWriteState(Torrent torrent)
        {
            torrent.addCommand(Torrent::writeState);
        }

        /**
         * creates "torrent::readState" command for the given torrent
         * and places it into the commands' queue of the torrent
         * to be executed later
         * @param torrent torrent
         */
        public void cmdTorrentReadState(Torrent torrent, Consumer<Boolean> callback)
        {
            torrent.addCommand((t) -> torrent.readState(callback));
        }

        /**
         * dumps active connections of the torrent to stdout
         */
        public void dump()
        {
            addCommand(torrent -> {
                Formatter formatter = new Formatter();
                formatter.format("torrent: %s\n", metainfo.infohash.toString());
                formatter.format("                              L  P                                 \n");
                formatter.format("                          C H CI CI   DLR  RQ   BLKS |   UPL  Q   BLKS     %%\n");

                connections.values().forEach(pc -> pc.dump(formatter));

//        formatter.format(" peer messages created: %d\n", PeerMessageCache.counter);
//        formatter.format("     buffers allocated: %d\n", SimpleFileStorage.buffersAllocated);
                formatter.format("                 state: %s\n", state.name());
                formatter.format("            completion: %.2f\n", 100.0 * pieces.cardinality() / metainfo.pieces);
                formatter.format("                 peers: %d\n", peers.size());
                //formatter.format("       save task queue: %d\n", SimpleFileStorage.exSave.getQueue().size());

                System.out.println(formatter.toString());
            });
        }

    }
















}
