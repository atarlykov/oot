package oot;

import oot.be.Metainfo;
import oot.dht.HashId;
import oot.dht.Node;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
     * key to save DHT state to storage
     */
    private static final String DHT_NODE_STATE_SAVE_KEY = "dht";

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
     * indicates if DHT(Node) is available of not
     */
    private boolean dhtEnabled;

    /**
     * instance of api class to work with trackers
     */
    TrackersManager trackersManager;

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
        torrents = new ArrayList<>();
        trackersManager = new TrackersManager(this);

        dhtEnabled = _dhtEnable;
        if (dhtEnabled) {
            byte[] state = storage.read(DHT_NODE_STATE_SAVE_KEY);
            if (state != null) {
                node = new Node(state);
            } else {
                node = new Node();
            }
        }
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
     * access outside the client's thread must be synchronized
     * @return ref to internal torrents collection
     */
    public List<Torrent> getTorrents() {
        return torrents;
    }

    /**
     * starts client processing inside the dedicated thread
     * @return true if resources were allocated successfully
     */
    public boolean start()
    {
        try {
            trackersManager.init();
        } catch (IOException e) {
            if (DEBUG) System.out.println("client: error starting trackersManager: " + e.getMessage());
            return false;
        }

        try {
            selector = Selector.open();
        } catch (IOException e) {
            if (DEBUG) System.out.println("client: error opening selector: " + e.getMessage());
            stop();
            return false;
        }

        if (dhtEnabled) {
            try {
                node.start();
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

        return true;
    }

    /**
     * stops client's dedicated thread
     */
    public void stop()
    {
        if (thread == null) {
            // has not been started
            return;
        }

        try {
            thread.active = false;
            // this will give us happens-before
            // for all subsequent operations
            thread.join();
        } catch (InterruptedException ignored) {}

        trackersManager.stop();

        if (dhtEnabled) {
            // save DHT state as we can't get in later from the air
            byte[] state = node.getStateWithBlocking();
            storage.write(DHT_NODE_STATE_SAVE_KEY, state);
            node.stop();
        }

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
        } catch (IOException ignored) {
            // todo
        }

        long now = System.currentTimeMillis();
        for (Torrent torrent: torrents) {
            if (TORRENT_UPDATE_TIMEOUT < now - torrent.timeLastUpdate) {
                torrent.update();
            }

            if (DEBUG) {
                if (TORRENT_DUMP_TIMEOUT < now - torrent.timeLastDump) {
                    torrent.timeLastDump = now;
                    torrent.dump();
                }
            }
        }

        if (SPEED_LIMITS_UPDATE_TIMEOUT < now - timeLastSpeedLimitsUpdate) {
            setSpeedLimitDownload(speedLimitDownload);
            timeLastSpeedLimitsUpdate = now;
        }

        if (isDhtEnabled())
        {
            if (DHT_NODE_STATE_SAVE_TIMEOUT < now - timeLastDHTStateSave)
            {
                timeLastDHTStateSave = now;

                if (node.bootstrapped) {
                    // save state only of node bootstrapped
                    // otherwise it's possible to ruin the state
                    node.getState(data -> {
                        storage.write(DHT_NODE_STATE_SAVE_KEY, data);
                    });
                }
            }
        }

        trackersManager.update();
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

    /**
     * extracts client name from hash id, supports not all the clients
     * @param id hash id of the client to decode
     * @return not null string name
     */
    public String extractClientNameFromId(HashId id)
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
    private String extractClientAsciiText(byte[] data, int position)
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

}
