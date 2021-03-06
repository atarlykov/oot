package oot.poc;

import oot.be.Metainfo;
import oot.dht.HashId;
import oot.dht.Node;
import oot.tracker.DhtTrackerFactory;
import oot.tracker.StandardTrackerFactory;
import oot.tracker.TrackerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class Client {

    private static final boolean DEBUG = true;

    /**
     * prefix used in new ID generation (due to spec),
     * bytes [0,1] are client type
     * bytes [3..6] are version
     */
    private static final byte[] ID_VERSION_PREFIX = new byte[] {'-', 'O', 'O', '0', '0', '1', '1', '-' };

    /**
     * timeout to call {@link Torrent#update()} method for each torrent registered
     */
    private static final long TORRENT_UPDATE_TIMEOUT = 1000;
    /**
     * timeout to dump state and connections of each torrent (debug)
     */
    private static final long TORRENT_DUMP_TIMEOUT = 10_000;
    /**
     * timeout to recalculate speed limits for all torrents and connections
     */
    private static final long SPEED_LIMITS_UPDATE_TIMEOUT = 10_000;
    /**
     * timeout to save DHT routing table state to storage
     */
    private static final long DHT_NODE_STATE_SAVE_TIMEOUT = 600_000;
    /**
     * key to save DHT state to storage
     */
    private static final String DHT_NODE_STATE_SAVE_KEY = "dht";

    /**
     * unique 'fixed' identifier of this client
     * todo: public is only to compile poc
     */
    public HashId id;

    /**
     * the only one selector per client,
     * is used to operate p2p connections of all torrents
     */
    Selector selector;
    /**
     * ref to storage api that is used for all
     * torrents' read/wrie operations, states save, etc.
     */
    Storage storage;
    /**
     * ref to DHT node if available
     */
    Node node;
    /**
     * indicates if DHT(Node) is available of not
     */
    private boolean dhtEnabled;

    /**
     * timestamp of the last update of speed limits
     */
    private long timeLastSpeedLimitsUpdate = 0;
    /**
     * timestamp of the last save of DHT routing table
     */
    private long timeLastDHTStateSave = 0;
    /**
     * global download speed limit for all torrents
     */
    private long speedLimitDownload;
    /**
     * global upload speed limit for all torrents
     */
    private long speedLimitUpload;

    /**
     * list of tracker factories used to resolve tracker urls
     * and provide tracker implementations, like http,udp,dht,pex,...
     */
    final List<TrackerFactory> trackerFactories = new ArrayList<>();

    /**
     * allowed constructor
     * @param _id unique id of this torrent
     * @param _storage ref to storage
     */
    public Client(HashId _id, Storage _storage)
    {
        this(_id, _storage, true);
    }

    /**
     * allowed constructor
     * @param _id unique id of this torrent
     * @param _storage ref to storage
     * @param _dhtEnable DHT support
     */
    public Client(HashId _id, Storage _storage, boolean _dhtEnable)
    {
        id = _id;
        // set version data due to spec
        System.arraycopy(ID_VERSION_PREFIX, 0, id.getBytes(), 0, ID_VERSION_PREFIX.length);

        storage = _storage;

        dhtEnabled = _dhtEnable;
        if (dhtEnabled) {
            byte[] state = storage.read(DHT_NODE_STATE_SAVE_KEY);
            if (state != null) {
                node = new Node(state);
            } else {
                node = new Node();
            }
        }

        // standard factories
        trackerFactories.add(new DhtTrackerFactory(node));
        //trackerFactories.add(new PexTrackerFactory(node));
        trackerFactories.add(new StandardTrackerFactory(id));
    }

    /**
     * @return true if DHT support is active
     */
    public boolean isDhtEnabled() {
        return dhtEnabled;
    }

    /**
     * @return ref to DHT node
     */
    public Node getDhtNode() {
        return node;
    }

    /**
     * tries to generate unique id for this client,
     * @return generated id
     */
    private HashId generateUniqueId() {
        byte[] data = new byte[HashId.HASH_LENGTH_BYTES];
        try {
            // this method is usually called only once, so just create random here
            SecureRandom random = SecureRandom.getInstanceStrong();
            random.reseed();
            random.nextBytes(data);
        } catch (NoSuchAlgorithmException e) {
            Random random = new Random();
            random.setSeed(System.nanoTime());
            random.nextBytes(data);
        }

        // set version data due to spec
        System.arraycopy(ID_VERSION_PREFIX, 0, data, 0, ID_VERSION_PREFIX.length);
        return HashId.wrap(data);
    }



    /**
     * calculates download speed across all registered torrents in bytes/sec
     * @param seconds number of seconds to average (if data is available)
     * @return bytes/sec
     */
//    public long getDownloadSpeed(int seconds) {
//        long total = 0;
//        for (int i = 0; i < torrents.size(); i++) {
//            Torrent torrent = torrents.get(i);
//            total += torrent.getDownloadSpeed(seconds);
//        }
//        return total;
//    }

    /**
     * calculates upload speed across all registered torrents in bytes/sec
     * @param seconds number of seconds to average (if data is available)
     * @return bytes/sec
     */
//    public long getUploadSpeed(int seconds) {
//        long total = 0;
//        for (int i = 0; i < torrents.size(); i++) {
//            Torrent torrent = torrents.get(i);
//            total += torrent.getUploadSpeed(seconds);
//        }
//        return total;
//    }

    /**
     * sets global download limit,
     * note: could be easily extended to support per torrent limits
     * @param limit number of bytes/sec
     */
//    public void setSpeedLimitDownload(long limit)
//    {
//        speedLimitDownload = limit;
//
//        if (torrents.size() == 0) {
//            return;
//        }
//
//        if (limit == 0) {
//            for (Torrent torrent : torrents) {
//                torrent.setDownloadSpeedLimit(0);
//            }
//            return;
//        }
//
//        // calculate total and per torrent bandwidths
//        long[] speeds = new long[torrents.size()];
//        long total = 0;
//        for (int i = 0; i < torrents.size(); i++) {
//            Torrent torrent = torrents.get(i);
//            speeds[i] = torrent.getDownloadSpeed(2);
//            total += speeds[i];
//        }
//
//        long averageSpeedLimit = total / speeds.length;
//        long availableSpeedBudget = speedLimitDownload - total;
//
//        int hightSpeedCount = 0;
//        for (int i = 0; i < torrents.size(); i++) {
//            Torrent torrent = torrents.get(i);
//            long speed = speeds[i];
//
//            if (speed < averageSpeedLimit - Torrent.BLOCK_LENGTH) {
//                // let this torrent to use more bandwidth,
//                // soft target is the same speed for all
//                torrent.setDownloadSpeedLimit(averageSpeedLimit);
//            } else {
//                hightSpeedCount++;
//            }
//        }
//
//        long averageSpeedBudget = availableSpeedBudget / hightSpeedCount;
//        for (int i = 0; i < torrents.size(); i++) {
//            Torrent torrent = torrents.get(i);
//            long speed = speeds[i];
//            if (averageSpeedLimit - Torrent.BLOCK_LENGTH <= speed) {
//                torrent.setDownloadSpeedLimit(speed + averageSpeedBudget);
//            }
//        }
//    }


    /**
     * collection of all the torrents tracked by the client
     */
    final ConcurrentHashMap<HashId, Torrent> torrents = new ConcurrentHashMap<>();

    /**
     * stored references to torrents in active state,
     * populated on torrents' start/stop/add/...
     * NOTE: accessed only from client thread
     */
    final ConcurrentHashMap<HashId, Torrent> active = new ConcurrentHashMap<>();
    /**
     * active torrents' runner threads
     */
    final List<TorrentRunnerThread> runners = new ArrayList<>();
    /**
     * collection of queues for each active torrent,
     * must be cleared eventually by client's main thread
     */
    final ConcurrentMap<Torrent, ConcurrentLinkedQueue<TRCmd>> runCmdQueues = new ConcurrentHashMap<>();
    /**
     * marker queue to sequentially notify runner threads
     * about new commands available to be processed for a torrent
     */
    final ConcurrentLinkedQueue<Torrent> runMarkerQueue = new ConcurrentLinkedQueue<>();
    /**
     * commands queue processed by client thread in {@link #update()}
     */
    final ConcurrentLinkedQueue<Runnable> clCmdQueue = new ConcurrentLinkedQueue<>();


    long timeLastActiveTorrentUpdate;
    long timeLastActiveDump;


    /**
     * base class for all commands, processed by torrent runners
     * todo: could be replaced with queue<Runnable>
     */
    static abstract class TRCmd {
        /**
         * method will be executed by a torrent runner,
         * under lock protection to access a torrent
         */
        abstract void execute();
    }

    /**
     * will call peer connection io processing
     */
    static class PeerConnectionCmd extends TRCmd
    {
        // ref to peer connection that will be processed
        PeerConnection pc;

        /**
         * allowed constructor
         * @param _pc ref to pc
         */
        public PeerConnectionCmd(PeerConnection _pc) {
            pc = _pc;
        }
        @Override
        void execute() {
            pc.onChannelReady();
        }
    }

    /**
     * will call #{@link Torrent#update()}
     */
    static class TorrentUpdateCmd extends TRCmd
    {
        // ref to torrent that will be processed
        Torrent torrent;

        /**
         * allowed constructor
         * @param _torrent ref to torrent
         */
        public TorrentUpdateCmd(Torrent _torrent) {
            torrent = _torrent;
        }
        @Override
        void execute() {
            torrent.update();
        }
    }

    /**
     * will call #{@link Torrent#dump()}
     */
    static class TorrentDumpCmd extends TRCmd
    {
        // ref to torrent that will be processed
        Torrent torrent;

        /**
         * allowed constructor
         * @param _torrent ref to torrent
         */
        public TorrentDumpCmd(Torrent _torrent) {
            torrent = _torrent;
        }
        @Override
        void execute() {
            torrent.dump();
        }
    }

    /**
     * will call #{@link Torrent#startTorrent()} ()}
     */
    static class TorrentStartCmd extends TRCmd
    {
        // ref to torrent that will be processed
        Torrent torrent;

        /**
         * allowed constructor
         * @param _torrent ref to torrent
         */
        public TorrentStartCmd(Torrent _torrent) {
            torrent = _torrent;
        }
        @Override
        void execute() {
            torrent.startTorrent();
        }
    }

    /**
     * threads used to process connections' updates for torrents
     * and other possible commands that must be executed in bounds
     * of a torrents' processing thread
     */
    class TorrentRunnerThread extends Thread {

        public TorrentRunnerThread() {
            setName("oot-runner");
        }

        @Override
        public void run()
        {
            while (true)
            {
                // get marker if exists
                Torrent torrent = runMarkerQueue.poll();
                if (torrent != null) {
                    // try lock, if unsuccessful - some other runner
                    // is processing this torrent after getting another marker,
                    // that's ok, let him process the queue
                    if (torrent.runnerLock.tryLock()) {
                        // process all commands for this torrent
                        // with lock protection
                        try {
                            ConcurrentLinkedQueue<TRCmd> commands = runCmdQueues.get(torrent);
                            if (DEBUG) {
                                //TRCmd peek = commands.peek();
                                //System.out.println(System.nanoTime() + "  [TR] (cycle)  queue size:" + commands.size() +
                                //        (peek != null ? "   " + peek.getClass() : ""));
                            }
                            TRCmd command = null;
                            while ((command = commands.poll()) != null) {
                                command.execute();
                            }
                        } finally {
                            torrent.runnerLock.unlock();
                        }
                    }
                } else {
                    // marker queue is empty,
                    // wait for new events
                    synchronized (runMarkerQueue) {
                        try {
                            //System.out.println(System.nanoTime() + "  [TR]  (wait)");
                            long tStart = System.nanoTime();
                            runMarkerQueue.wait(100);
                            long tEnd = System.nanoTime();
                            System.out.println("[TR] time:" + (tEnd - tStart)/1000000);
                        } catch (InterruptedException ignored) {
                            // seems controlling client wants
                            // to stop this instance
                            break;
                        }
                    }
                }

                // separate check in case if queue is always full,
                // but we must stop processing
                if (Thread.interrupted()) {
                    break;
                }
            }
        }
    }


    /**
     * thread that handles client's updates
     * in background
     */
    private class ClientThread extends Thread
    {

        public ClientThread() {
            setName("oot-client");
        }

        // state flag to stop the thread
        volatile boolean active = true;

        @Override
        public void run() {
            while (active) {
                update();
            }
            Client.this._stop();
        }
    }

    /**
     * ref to running thread
     */
    private volatile ClientThread thread;

    /**
     * main client's method, called periodically by the service
     * thread to perform IO operation on connections and update
     * states of al linked torrents
     */
    private void update()
    {
        // distribute connections per torrents' specific queues
        // to be processed by torrent runners
        try {

//            if (DEBUG) {
//                selector.keys().stream().forEach(key ->
//                        System.out.println("[client]  (r:" + key.readyOps() + " i:" + key.interestOps() + ")" )
//                );
//            }

            // by default use small timeout, but
            // increase a lot in case id active
            // todo: include seeding/speed check
            long selectTimeout = 1;
            if (active.isEmpty()) {
                selectTimeout = 1000;
            }

            long tSelectStart = System.nanoTime();
            // The problem is that while we are selecting key with (ready:x interest:0)
            // it could be updated to (r:x i:5), but we still will be selecting till
            // the timeout expires and only the next select will fire...
            // [client] count:0  time:timeout  (was r:5 i:0)  (sel r:5 i:1)  <-- key modified during select
            // [client] count:1  time:0        (was r:5 i:1)  (sel r:1 i:1)
            // (selector impl doesn't guarantee to fire on modified keys)
            // And it stands for any interest at the moment of select call that in is not ready during selection,
            // so why selections must be not very seldom.
            // Must not the case when there are many connections
            selector.selectedKeys().clear();
            int count = selector.select(selectTimeout);
            long tSelectEnd = System.nanoTime();

//            if (DEBUG) {
//                selector.keys().stream().forEach(key ->
//                        System.out.println("[client]" +
//                                "  (r:" + key.readyOps() + " i:" + key.interestOps() + ")" +
//                                "  time:" + (tSelectEnd - tSelectStart)/1000000 +
//                                "  count:" + count
//                        ));
//            }


            if (DEBUG && (count == 0) && (tSelectEnd - tSelectStart < 100000)) {
                System.out.println(System.nanoTime() + "  [client] selected:0  time:" + (tSelectEnd - tSelectStart));
            }

            if (0 < count) {
                Set<SelectionKey> keys = selector.selectedKeys();
                keys.forEach(sKey -> {
                    // switch of all interest for the key,
                    // otherwise it will fire each time in this thread
                    // till it's handled in processor thread,
                    // interest must be set in processing
                    sKey.interestOpsAnd(0);
                    PeerConnection pc = (PeerConnection) sKey.attachment();
                    // add command to process specific torrent/connection,
                    // a number of commands is created... could be optimized
                    sendCommandToTorrent(pc.torrent, new PeerConnectionCmd(pc));
                });
            }

        } catch (IOException ignored) {
            // todo
        }

        long now = System.currentTimeMillis();
        if (timeLastActiveTorrentUpdate + TORRENT_UPDATE_TIMEOUT < now) {
            active.forEach( (hash, torrent) -> {
                // add another hard time limit ???
                if (isTorrentCmdQueueEmpty(torrent)) {
                    sendCommandToTorrent(torrent, new TorrentUpdateCmd(torrent));
                }
            });
            timeLastActiveTorrentUpdate = now;
        }


        if (timeLastActiveDump + TORRENT_DUMP_TIMEOUT < now) {
            active.forEach( (hash, torrent) -> sendCommandToTorrent(torrent, new TorrentDumpCmd(torrent)) );
            timeLastActiveDump = now;
        }

/*
        if (isDhtEnabled())
        {
            if (DHT_NODE_STATE_SAVE_TIMEOUT < now - timeLastDHTStateSave)
            {
                timeLastDHTStateSave = now;

                // save state only of node bootstrapped
                // otherwise it's possible to ruin the state
                node.getState(data -> {
                    // could be null if not bootstrapped
                    if (data != null) {
                        storage.write(DHT_NODE_STATE_SAVE_KEY, data);
                    }
                });
            }
        }
*/

        // todo: runners' management, start/interrupt


        // process commands that must be run
        // in bounds of the main client's thread
        Runnable cmd = null;
        while ((cmd = clCmdQueue.poll()) != null) {
            try {
                cmd.run();
            } catch (Throwable ignored) {}
        }
    }

    /**
     * starts client processing inside the dedicated thread,
     * must be called on start and only once
     * @return true if resources were allocated successfully
     */
    public boolean start()
    {
        if (thread != null) {
            return true;
        }
/*
        try {
            trackersManager.init();
        } catch (IOException e) {
            if (DEBUG) System.out.println("client: error starting trackersManager: " + e.getMessage());
            return false;
        }
*/

        try {
            selector = Selector.open();
        } catch (IOException e) {
            if (DEBUG) System.out.println("client: error opening selector: " + e.getMessage());
            stop();
            return false;
        }

        if (dhtEnabled) {
            try {
                node.start(true);
            } catch (IOException e) {
                if (DEBUG) System.out.println("client: error starting DHT node: " + e.getMessage());
                stop();
                return false;
            }
        }

        // run main thread
        thread = new ClientThread();
        thread.setDaemon(true);
        thread.start();


        manageRunnerThreads();

        return true;
    }

    /**
     * stops client's dedicated thread
     */
    public void stop()
    {
        if (thread == null) {
            return;
        }
        // this will stop main thread
        // and calls _stop
        thread.active = false;
        try {
            // this will give us happens-before
            // for all subsequent operations
            thread.join();
        } catch (InterruptedException ignored) {
        }

        // indicate we are stopped
        thread = null;
    }

    /**
     * internal method that's called on client thread's stop,
     * performs various cleanup in bounds of the main processing thread
     */
    private void _stop()
    {

/*
        trackersManager.stop();

        if (dhtEnabled) {
            // save DHT state as we can't get in later from the air
            byte[] state = node.getStateWithBlocking();
            storage.write(DHT_NODE_STATE_SAVE_KEY, state);
            node.stop();
        }
*/

        runners.forEach( Thread::interrupt);
        runners.forEach( thread -> {
            try {
                thread.join();
            } catch (InterruptedException ignored) {
            }
        });

        try {
            storage.stop();
        } catch (Exception e) {
        }
    }

    /**
     * add command to torrent's commands' queue to be processed
     * by a runner thread and appends a marker to marker queue
     * to allow runner threads have fair mode
     *
     * @param torrent torrent to send cmd to
     * @param cmd cmd to send
     */
    private void sendCommandToTorrent(Torrent torrent, TRCmd cmd)
    {
        runCmdQueues.computeIfAbsent(
                torrent,
                t -> new ConcurrentLinkedQueue<>()
        ).add(cmd);

        synchronized (runMarkerQueue) {
            // add marker to process the torrent
            runMarkerQueue.add(torrent);
            runMarkerQueue.notify();
        }
    }

    /**
     * @param torrent torrent to check command queue for
     * @return if command queue for the specified torrent is missing or empty
     */
    private boolean isTorrentCmdQueueEmpty(Torrent torrent)
    {
        ConcurrentLinkedQueue<TRCmd> trCmds = runCmdQueues.get(torrent);
        if (trCmds == null) {
            return true;
        }
        return trCmds.isEmpty();
    }

    /**
     * adds another torrent to be managed by the client
     * @param info parsed meta info of the torrent
     * @return ref to registered torrent
     */
    public Torrent addTorrent(Metainfo info, Consumer<Boolean> cb)
    {
        TorrentStorage tStorage = storage.getStorage(info);
        Torrent torrent = new Torrent(id, info, selector, tStorage, new ArrayList<>() /*, cb*/);
        torrents.put(info.infohash, torrent);
        startTorrent(info.infohash);

        manageRunnerThreads();
        return torrent;
    }

    /**
     * sends "start" command to the torrent identified by the hash
     * @param hash torrent hash
     */
    public void startTorrent(HashId hash)
    {
        Torrent torrent = torrents.get(hash);
        if (torrent != null) {
            active.putIfAbsent(hash, torrent);
        }
        sendCommandToTorrent(torrent, new TorrentStartCmd(torrent));
    }

    /**
     * manages set of threads' runner threads,
     * increases and decreases them as needed
     * todo: implement
     */
    private void manageRunnerThreads()
    {
        synchronized (runners)
        {
            if (runners.isEmpty() && !active.isEmpty()) {
                TorrentRunnerThread runner = new TorrentRunnerThread();
                runner.setDaemon(true);
                runner.start();
                runners.add(runner);
            }
        }
    }

}
