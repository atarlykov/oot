package oot;

import oot.be.Metainfo;
import oot.dht.HashId;
import oot.dht.Node;

import java.io.IOException;
import java.nio.channels.Selector;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

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
    private static final long TORRENT_UPDATE_TIMEOUT = 100;
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
     * unique 'fixed' identifier of this client
     */
    HashId id;

    // list of torrents [or map]
    private List<Torrent> torrents;

    /**
     * service executor available for torrents,
     * to be used for some tasks
     */
    ExecutorService executor = new ThreadPoolExecutor(
            2, 16,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>());


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
     * thread that handles client's updates
     * in background
     */
    private class ClientThread extends Thread
    {
        // state flag to stop the thread
        volatile boolean active = true;

        @Override
        public void run() {
            while (active) {
                update();
            }
        }
    }

    /**
     * ref to running thread
     */
    private ClientThread thread;

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
     * allowed constructor
     * @param _id unique id of this torrent
     * @param _storage ref to storage
     */
    public Client(HashId _id, Storage _storage /*, Executors?*/)
    {
        id = _id;
        // set version data due to spec
        System.arraycopy(ID_VERSION_PREFIX, 0, id.getBytes(), 0, ID_VERSION_PREFIX.length);

        storage = _storage;
        torrents = new ArrayList<>();
    }

    /**
     * allowed constructor
     * @param _storage ref to storage
     */
    public Client(Storage _storage /*, Executors?*/)
    {
        id = generateUniqueId();
        storage = _storage;
        torrents = new ArrayList<>();
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
     * adds another torrent to be managed by the client
     * @param info parsed meta info of the torrent
     * @return ref to registered torrent
     */
    Torrent addTorrent(Metainfo info) {
        Storage.TorrentStorage tStorage = storage.getStorage(info);
        Torrent torrent = new Torrent(this, info, tStorage);
        torrents.add(torrent);
        return torrent;
    }

    /**
     * starts client processing inside the dedicated thread
     * @return true if resources were allocated successfully
     */
    public boolean start()
    {
        try {
            selector = Selector.open();
        } catch (IOException e) {
            if (DEBUG) System.out.println("client start: " + e.getMessage());
            return false;
        }

        byte[] dhtState = storage.read("dht");
        if (dhtState != null) {
            node = new Node(dhtState);
        } else {
            node = new Node();
        }

        thread = new ClientThread();
        thread.setDaemon(true);
        thread.start();

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

        try {
            thread.active = false;
            thread.join();
        } catch (InterruptedException ignored) {
        }

        byte[] data = node.getStateWithBlocking();
        node.stop();

        storage.write("dht", data);
        storage.stop();
    }


    /**
     * main client's method, called periodically by the service
     * thread to perform IO operation on connections and update
     * states of al linked torrents
     */
    private void update()
    {
        try {
            // for all torrents
            selector.select(sKey -> {
                PeerConnection p = (PeerConnection) sKey.attachment();
                p.onChannelReady();
            }, 10);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long now = System.currentTimeMillis();
        for (Torrent torrent: torrents) {
            if (TORRENT_UPDATE_TIMEOUT < now - torrent.timeLastUpdate) {
                torrent.update();
            }

            if (DEBUG) {
                if (TORRENT_DUMP_TIMEOUT < now - torrent.timeLastDump) {
                    torrent.dump();
                }
            }
        }

        if (SPEED_LIMITS_UPDATE_TIMEOUT < now - timeLastSpeedLimitsUpdate) {
            setSpeedLimitDownload(speedLimitDownload);
            timeLastSpeedLimitsUpdate = now;
        }

        if (DHT_NODE_STATE_SAVE_TIMEOUT < now - timeLastDHTStateSave) {
            timeLastDHTStateSave = now;
            node.getState(data -> {
                storage.write("dht", data);
            });
        }
    }

    /**
     * calculates download speed across all registered torrents in bytes/sec
     * @param seconds number of seconds to average (if data is available)
     * @return bytes/sec
     */
    public long getDownloadSpeed(int seconds) {
        long total = 0;
        for (int i = 0; i < torrents.size(); i++) {
            Torrent torrent = torrents.get(i);
            total += torrent.getDownloadSpeed(seconds);
        }
        return total;
    }

    /**
     * calculates upload speed across all registered torrents in bytes/sec
     * @param seconds number of seconds to average (if data is available)
     * @return bytes/sec
     */
    public long getUploadSpeed(int seconds) {
        long total = 0;
        for (int i = 0; i < torrents.size(); i++) {
            Torrent torrent = torrents.get(i);
            total += torrent.getUploadSpeed(seconds);
        }
        return total;
    }

    /**
     * sets global download limit,
     * note: could be easily extended to support per torrent limits
     * @param limit number of bytes/sec
     */
    public void setSpeedLimitDownload(long limit)
    {
        speedLimitDownload = limit;

        if (torrents.size() == 0) {
            return;
        }

        if (limit == 0) {
            for (Torrent torrent : torrents) {
                torrent.setDownloadSpeedLimit(0);
            }
            return;
        }

        // calculate total and per torrent bandwidths
        long[] speeds = new long[torrents.size()];
        long total = 0;
        for (int i = 0; i < torrents.size(); i++) {
            Torrent torrent = torrents.get(i);
            speeds[i] = torrent.getDownloadSpeed(2);
            total += speeds[i];
        }

        long averageSpeedLimit = total / speeds.length;
        long availableSpeedBudget = speedLimitDownload - total;

        int hightSpeedCount = 0;
        for (int i = 0; i < torrents.size(); i++) {
            Torrent torrent = torrents.get(i);
            long speed = speeds[i];

            if (speed < averageSpeedLimit - Torrent.BLOCK_LENGTH) {
                // let this torrent to use more bandwidth,
                // soft target is the same speed for all
                torrent.setDownloadSpeedLimit(averageSpeedLimit);
            } else {
                hightSpeedCount++;
            }
        }

        long averageSpeedBudget = availableSpeedBudget / hightSpeedCount;
        for (int i = 0; i < torrents.size(); i++) {
            Torrent torrent = torrents.get(i);
            long speed = speeds[i];
            if (averageSpeedLimit - Torrent.BLOCK_LENGTH <= speed) {
                torrent.setDownloadSpeedLimit(speed + averageSpeedBudget);
            }
        }
    }

}
