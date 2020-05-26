package oot;

import oot.be.BEParser;
import oot.be.BEValue;
import oot.dht.HashId;

import java.io.IOException;
import java.net.*;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * facade class to communicate with trackers via various protocols
 */
class TrackersManager {

    private final static boolean DEBUG = true;
    private final static long TRACKERS_CHECK_TIMEOUT = 1000;

    /**
     * period to consider received connection id valid
     */
    private final static long UDP_CONNECTION_ID_TIMEOUT = 60 * 1000;

    private final static long UDP_PROTOCOL_MAGIC_ID = 0x41727101980L;

    /**
     * ref to executor service for running
     * background tasks (http client, etc)
     */
    //ExecutorService executor;
    /**
     * ref to the parent client
     */
    Client client;
    /**
     * http client to communicate with trackers via http/https protocols
     */
    HttpClient http;
    /**
     * udp channel to support udp trackers
     */
    private DatagramChannel channel;
    /**
     * buffer for udp datagrams
     */
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(2048).order(ByteOrder.BIG_ENDIAN);

    /**
     * timestamp of the last check of active torrents
     * for outdated trackers
     */
    private long timeLastTrackerCheck;

    /**
     * internal descriptor of all submitted operations
     * that must track state while being processed
     */
    private class Operation
    {
        // alias for internal usage, indicates operation must be finished
        static final boolean OPERATION_FINISH = true;
        // alias for internal usage, indicates operation must be proceeded with execution
        static final boolean OPERATION_RERUN = false;

        /**
         * transaction id
         */
        int tx;
        /**
         * ref to root tracker for this operation
         */
        Tracker tracker;
        /**
         * as all operations and non-blocking,
         * we need a callback to notify on finish
         */
        Consumer<Boolean> callback;

        // timeout could be different for various operations
        long timeout;
        // creation timestamp
        long timestamp;

        /**
         * allowed constructor
         * @param timeout operation timeout
         * @param tracker tracker we are working with
         * @param callback optional callback
         */
        public Operation(long timeout, Tracker tracker, Consumer<Boolean> callback) {
            this.tx = nextTransactionId();
            this.tracker = tracker;
            this.callback = callback;
            this.timeout = timeout;
            this.timestamp = System.currentTimeMillis();
        }

        /**
         * called to update state of the operation
         * @return true if operation must be finished and false otherwise
         */
        boolean update() {
            return OPERATION_RERUN;
        }

        /**
         * called to parse received message with tx id of the operation
         * @param buffer buffer with the message
         * @return true if operation must be finished and false otherwise
         */
        boolean receive(ByteBuffer buffer) {
            return OPERATION_RERUN;
        }
    }

    /**
     * operation runs only once with a timeout,
     * spec variant (15*2**n) is implemented with updates via top level operations
     */
    private class UDPConnectOperation extends Operation
    {
        static final long OPERATION_TIMEOUT = 400_000;
        boolean messageSent;
        InetSocketAddress address;

        public UDPConnectOperation(long timeout, Tracker tracker, InetSocketAddress address, Consumer<Boolean> callback) {
            super(timeout, tracker, callback);
            this.address = address;
        }

        public UDPConnectOperation(Tracker tracker, InetSocketAddress address, Consumer<Boolean> callback) {
            this(OPERATION_TIMEOUT, tracker, address, callback);
        }

        /**
         * populates the specified buffer with the message
         * @param buffer buffer to populate
         */
        private void populateSendBuffer(ByteBuffer buffer) {
            buffer.clear();
            buffer.putLong(UDP_PROTOCOL_MAGIC_ID);
            buffer.putInt(0);
            buffer.putInt(tx);
            buffer.flip();
        }

        @Override
        boolean update()
        {
            long now = System.currentTimeMillis();

            if (timestamp + timeout < now) {
                return OPERATION_FINISH;
            }
            if (messageSent) {
                return OPERATION_RERUN;
            }

            populateSendBuffer(buffer);

            boolean sent = send(address);
            if (!sent) {
                // ignore buffer overflow, etc.
                // just stop the operation
                if (callback != null) {
                    callback.accept(false);
                }
                return OPERATION_FINISH;
            }

            messageSent = true;
            return OPERATION_RERUN;
        }

        @Override
        boolean receive(ByteBuffer buffer)
        {
            if (buffer.remaining() != 16) {
                if (callback != null) {
                    callback.accept(false);
                }
                return OPERATION_FINISH;
            }
            int action = buffer.getInt(0);
            tracker.udpConnectionId = buffer.getLong(8);
            tracker.udpConnectionTime = System.currentTimeMillis();

            if (callback != null) {
                callback.accept(true);
            }

            return OPERATION_FINISH;
        }
    }

    /**
     * announces torrent via the specified tracker
     */
    private class UDPAnnounceOperation extends Operation
    {
        static final long OPERATION_TIMEOUT = 600_000;
        /**
         * type of announce
         */
        private AnnounceEvent event;
        /**
         * address parsed from tracker url
         */
        private InetSocketAddress address;
        /**
         * indicator flag that we are waiting for a connect operation to finish
         */
        private boolean connecting;
        /**
         * indicates we have sent announce message
         */
        boolean messageSent;
        /**
         * indicator flag that something has failed and operation must be terminated
         */
        private boolean failed;

        /**
         * allowed constructor
         * @param timeout operation specific timeout
         * @param tracker tracker to announce state too
         * @param event announce event
         * @param callback optional callback
         */
        public UDPAnnounceOperation(long timeout, Tracker tracker, AnnounceEvent event, Consumer<Boolean> callback)
        {
            super(timeout, tracker, callback);
            this.event = event;

            String url = tracker.getCurrentUrl();
            if (!url.startsWith("udp://")) {
                failed = true;
                return;
            }

            // there are tracker urls like udp://a.b.c:1/announce
            // we ignore 'announce' part


            url = url.substring(6);

            // this handles ipv6 in machine form ad others
            int index = url.lastIndexOf(':');
            if (index == -1) {
                failed = true;
                return;
            }

            String addr = url.substring(0, index);
            int port = port(url, index + 1);

            //if (url.contains("ipv6") || url.contains("ip6")) {
            //}
            try {
                address = new InetSocketAddress(InetAddress.getByName(addr), port);
            } catch (Exception e) {
                failed = true;
            }
        }

        /**
         * allowed constructor,
         * default timeout is used
         * @param tracker tracker to announce state too
         * @param event announce event
         * @param callback optional callback
         */
        public UDPAnnounceOperation(Tracker tracker, AnnounceEvent event, Consumer<Boolean> callback) {
            this(OPERATION_TIMEOUT, tracker, event, callback);
        }

        /**
         * extracts port number from the string
         * @param url base string to extract from
         * @param index starting index of the port part
         * @return port value
         */
        private int port(String url, int index) {
            int port = 0;
            while ((index < url.length()) && Character.isDigit(url.charAt(index))) {
                port = port * 10 + Character.digit(url.charAt(index), 10);
                index++;
            }
            return port;
        }

        /**
         * populates the specified buffer with the message
         * assuming all data is available
         * @param buffer buffer to populate
         */
        private void populateSendBuffer(ByteBuffer buffer)
        {
            /*
                0       64-bit integer  connection_id
                8       32-bit integer  action          1 // announce
                12      32-bit integer  transaction_id
                16      20-byte string  info_hash
                36      20-byte string  peer_id
                56      64-bit integer  downloaded
                64      64-bit integer  left
                72      64-bit integer  uploaded
                80      32-bit integer  event           0 // 0: none; 1: completed; 2: started; 3: stopped
                84      32-bit integer  IP address      0 // default
                88      32-bit integer  key
                92      32-bit integer  num_want        -1 // default
                96      16-bit integer  port
             */
            buffer.clear();
            buffer.putLong(tracker.udpConnectionId);
            buffer.putInt(1);
            buffer.putInt(tx);
            buffer.put(tracker.torrent.metainfo.getInfohash().getBytes());
            buffer.put(client.id.getBytes());
            long downloaded = tracker.torrent.downloaded.get();
            buffer.putLong(downloaded);
            buffer.putLong(tracker.torrent.metainfo.length - downloaded);
            buffer.putLong(tracker.torrent.uploaded.get());
            buffer.putInt(event.value); //event  0: none; 1: completed; 2: started; 3: stopped
            buffer.putInt(0);
            buffer.putInt(0);
            buffer.putInt(-1);
            buffer.putShort((short)0);
            buffer.flip();
        }

        @Override
        boolean update()
        {
            if (failed) {
                // initial validation or child operation failed
                return OPERATION_FINISH;
            }

            // global op timeout
            long now = System.currentTimeMillis();
            if (timestamp + timeout < now) {
                if (callback != null) {
                    callback.accept(false);
                }
                return OPERATION_FINISH;
            }

            if (connecting) {
                // still waiting for child op
                return OPERATION_RERUN;
            }

            if (messageSent) {
                // have already sent the message,
                // wait for receive() to be called
                return OPERATION_RERUN;
            }


            if (tracker.udpConnectionTime + UDP_CONNECTION_ID_TIMEOUT < now)
            {
                // connection id expired, request new one
                connecting = true;
                operations.add(new UDPConnectOperation(tracker, address, result -> {
                    if (result) {
                        connecting = false;
                    } else {
                        if (callback != null) {
                            callback.accept(false);
                        }
                        // end the top op
                        failed = true;
                    }
                }));
                return OPERATION_RERUN;
            }

            populateSendBuffer(buffer);
            boolean result = send(address);
            if (!result) {
                return OPERATION_RERUN;
            }
            messageSent = true;

            return OPERATION_RERUN;
        }

        @Override
        boolean receive(ByteBuffer buffer)
        {
            /*
                0           32-bit integer  action          1 // announce
                4           32-bit integer  transaction_id
                8           32-bit integer  interval
                12          32-bit integer  leechers
                16          32-bit integer  seeders
                20 + 6 * n  32-bit integer  IP address
                24 + 6 * n  16-bit integer  TCP port
                20 + 6 * N
             */
            if (buffer.remaining() < 20) {
                if (callback != null) {
                    callback.accept(false);
                }
                return OPERATION_FINISH;
            }
            int action = buffer.getInt(0);

            tracker.interval = buffer.getInt(8);
            tracker.leechers = buffer.getInt(12);
            tracker.seeders = buffer.getInt(16);

            buffer.position(20);
            Set<InetSocketAddress> peers = new HashSet<>();
            try {
                byte[] addr;
                if (address.getAddress() instanceof Inet4Address) {
                    addr = new byte[4];
                } else {
                    addr = new byte[16];
                }

                while (addr.length + 2 < buffer.remaining()) {
                    buffer.get(addr);
                    int port = buffer.getShort() & 0xFFFF;
                    InetSocketAddress pAddr = new InetSocketAddress(InetAddress.getByAddress(addr), port);
                    peers.add(pAddr);
                }

            } catch (Exception ignored) {
                // must not happen as we don't resolve addresses
            }

            // this is optional as updated when op is created
            tracker.timeLastAnnounce = System.currentTimeMillis();

            if (callback != null) {
                callback.accept(true);
            }

            return OPERATION_FINISH;
        }
    }


    /**
     * collection of active operations,
     */
    List<Operation> operations = new ArrayList<>();

    int tx;

    int nextTransactionId() {
        // todo random
        tx += 1;
        return tx;
    }

    /**
     * service method, sends prepared send buffer
     * @param address destination address to send too
     * @return true if datagram was successfully sent and
     * false otherwise (that could be buffer overflow or some io error)
     */
    private boolean send(InetSocketAddress address) {
        try {
            if (DEBUG) TrackersManager.dumpMessage(true, false, buffer, address);

            int sent = channel.send(buffer, address);
            if (sent == 0)
            {
                // there were no space in system send buffer,
                // exit and let retry on the next update turn
                return false;
            }

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public TrackersManager(Client _client) {
        client = _client;
    }

    public void init() throws IOException {
        channel = DatagramChannel.open(StandardProtocolFamily.INET);
        channel.bind(null);
        //InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
        //channel.setOption(StandardSocketOptions.SO_RCVBUF, );
        //channel.setOption(StandardSocketOptions.SO_SNDBUF, );
        channel.configureBlocking(false);
    }

    /**
     * must be called in bounds of the same thread as update()
     */
    public void stop()
    {
        // force drop all operations in progress
        operations.clear();

        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {}
        }

        // it's possible we have http operations executed via client' executor
    }

    enum AnnounceEvent {
        NONE(0),
        COMPLETED(1),
        STARTED(2),
        STOPPED(3);

        int value;
        AnnounceEvent(int _value) {
            value = _value;
        }
    }

    public void announce(Torrent torrent, AnnounceEvent event) {
        for(Tracker tracker: torrent.trackers) {
            announce(tracker, event);
        }
    }

    public void announce(Tracker tracker, AnnounceEvent event)
    {
        String url = tracker.getCurrentUrl();
        if (url.startsWith("http://") || url.startsWith("https://")) {
            String query = tracker.buildAnnounceUrl(url);
            client.executor.submit(() -> {
                try {
                    BEValue beValue = httpAnnounce(query);
                    Set<Peer> peers = httpAnnounceParseResponse(beValue);
                    tracker.torrent.addPeers(peers);
                } catch (Exception e) {
                    tracker.switchToNextUrl();
                }  finally {
                }
            });
        }
        else if (url.startsWith("udp://")) {
            operations.add(new UDPAnnounceOperation(tracker, event, null));
        }
    }


    public void scrape(Tracker t) {
    }


    void updateActiveTorrents()
    {
        long now = System.currentTimeMillis();

        List<Torrent> torrents = client.getTorrents();
        for (int i = 0; i < torrents.size(); i++) {
            Torrent torrent = torrents.get(i);

            for (Tracker tracker : torrent.trackers)
            {
                if (Tracker.TIMEOUT_ANNOUNCE < now - tracker.timeLastAnnounce) {
                    // update time right here to prevent double submissions
                    tracker.timeLastAnnounce = System.currentTimeMillis();
                    announce(tracker, AnnounceEvent.NONE);
                }
                if (Tracker.TIMEOUT_SCRAPE < now - tracker.timeLastScrape) {
                    tracker.timeLastScrape = now;
                }
            }
        }
    }


    void update()
    {
        long now = System.currentTimeMillis();

        // generate new announce/scrape requests
        if (TRACKERS_CHECK_TIMEOUT < now - timeLastTrackerCheck) {
            updateActiveTorrents();
        }

        int counter = 16;

        try {
            while (0 < counter)
            {
                counter -= 1;

                SocketAddress address;

                buffer.clear();
                // channel is in non-blocking mode, so this will not block
                if ((address = channel.receive(buffer)) == null) {
                    // nothing more in the channel
                    break;
                }

                buffer.flip();
                if (DEBUG) TrackersManager.dumpMessage(false, false, buffer, (InetSocketAddress) address);

                if (buffer.remaining() < 8) {
                    continue;
                }

                int tx = buffer.getInt(4);

                for (int i = 0; i < operations.size(); i++) {
                    Operation operation = operations.get(i);
                    if (tx == operation.tx) {
                        boolean finished = operation.receive(buffer);
                        if (finished) {
                            operations.remove(i);
                        }
                        break;
                    }
                }
            }
        } catch (IOException ignored) {
                // todo
        }

        for (int i = operations.size() - 1; 0 <= i; i--) {
            Operation operation =  operations.get(i);
            boolean finished = operation.update();
            if (finished) {
                operations.remove(i);
            }
        }
    }




    /**
     * performs http(s) request to the specified url and
     * returns server response in BE form
     * @param query url to query
     * @return parsed server's response
     * @throws Exception on any IO error
     */
    private static BEValue httpAnnounce(String query)
            throws Exception
    {
        System.out.println(query);
        URL url = new URL(query);

        URLConnection urlConnection = url.openConnection();
        urlConnection.setConnectTimeout(5000);
        urlConnection.connect();

        //if (DEBUG) System.out.println("response code: " + ((HttpURLConnection)urlConnection).getResponseCode());

        if (((HttpURLConnection)urlConnection).getResponseCode() != 200) {
            return null;
        }
        //((HttpURLConnection)urlConnection).getResponseMessage();

        byte[] data = urlConnection.getInputStream().readAllBytes();
        ByteBuffer b = ByteBuffer.wrap(data);
        BEParser parser = new BEParser();
        BEValue response = parser.parse(b);
        return response;
    }

    /**
     * parses tracker response as sent as answer to announce request
     * @param data parsed binary encoded representation
     * @return not null set with unique peers
     */
    public Set<Peer> httpAnnounceParseResponse(BEValue data)
    {
        if ((data == null) || !data.isDict() || !data.isDictNotEmpty()) {
            return Collections.emptySet();
        }
        BEValue beReason = data.dictionary.get("failure reason");
        if (beReason != null) {
            String reason = beReason.getBStringAsString();
            return Collections.emptySet();
        }

        // todo: support later
        //data.dictionary.get("interval");

        Set<Peer> result = new HashSet<>();

        BEValue peers = data.dictionary.get("peers");
        if (BEValue.isList(peers)) {
            // bep0003, this is list of dictionaries: [{peer id, ip, port}]
            for (int i = 0; i < peers.list.size(); i++) {
                BEValue bePeer = peers.list.get(i);
                if (!BEValue.isDict(bePeer)) {
                    continue;
                }
                BEValue peer_id = bePeer.dictionary.get("peer id");
                BEValue ip = bePeer.dictionary.get("ip");
                BEValue port = bePeer.dictionary.get("port");

                if (BEValue.isBString(ip) && BEValue.isBString(port)) {
                    try {
                        InetSocketAddress isa = new InetSocketAddress(
                                Inet4Address.getByName(ip.getBStringAsString()),
                                Integer.parseInt(port.getBStringAsString())
                        );
                        Peer peer = new Peer(isa);
                        result.add(peer);
                    } catch (UnknownHostException ignore) {
                    }
                }
            }
            return result;
        }

        if (BEValue.isBString(peers)) {
            // bep0023 compact representation ipv4 addressed "4+2|.."
            byte[] tmp = new byte[4];
            for (int i = 0; i < peers.bString.length / 6; i++) {
                try {
                    tmp[0] = peers.bString[i*6 + 0];
                    tmp[1] = peers.bString[i*6 + 1];
                    tmp[2] = peers.bString[i*6 + 2];
                    tmp[3] = peers.bString[i*6 + 3];

                    int port = (Byte.toUnsignedInt(peers.bString[i*6 + 4]) << 8)
                            | Byte.toUnsignedInt(peers.bString[i*6 + 5]);

                    InetSocketAddress isa = new InetSocketAddress( Inet4Address.getByAddress(tmp), port);
                    Peer peer = new Peer(isa);
                    result.add(peer);
                } catch (UnknownHostException ignored) {
                }
            }
            return result;
        }

        return Collections.emptySet();
    }

    /**
     * parses binary ip4 address format, 4 bytes address and 2 bytes port as big-endian,
     * @param buffer buffer with data, remaining size must be of length 6 at least
     * @return parsed address or null if something missing or wrong
     */
    InetSocketAddress parseIPv4Address(ByteBuffer buffer)
    {
        byte[] addr = new byte[4];
        buffer.get(addr);
        int port = buffer.getShort() & 0xFFFF;

        try {
            return new InetSocketAddress(Inet4Address.getByAddress(addr), port);
        } catch (UnknownHostException e) {
            // that shouldn't be as we don't resolve address
            return null;
        }
    }

    /**
     * parses binary ip6 address format, 16 bytes address and 2 bytes port as big-endian,
     * @param buffer buffer with data, remaining size must be of length 18 at least
     * @return parsed address or null if something missing or wrong
     */
    InetSocketAddress parseIPv6Address(ByteBuffer buffer)
    {
        byte[] addr = new byte[16];
        buffer.get(addr);
        int port = buffer.getShort() & 0xFFFF;

        try {
            return new InetSocketAddress(Inet6Address.getByAddress(addr), port);
        } catch (UnknownHostException e) {
            // that shouldn't be as we don't resolve address
            return null;
        }
    }

    private static void dumpMessage(boolean outbound, boolean skipped, ByteBuffer buffer, InetSocketAddress address)
    {
        StringBuilder builder = new StringBuilder();
        if (outbound) {
            if (skipped) {
                builder.append(">>XX ");
            } else {
                builder.append(">>-- ");
            }
        } else {
            builder.append("--<< ");
        }

        builder.append(address.toString());

        int action = -1;
        int tx = -1;

        if (outbound) {
            if (buffer.remaining() < 16) {
                builder.append(" incorrect length");
                System.out.println(builder.toString());
                return;
            }
            long cid = buffer.getLong(0);
            action = buffer.getInt(8);
            tx = buffer.getInt(12);
            builder.append("    tx:").append(tx);
            builder.append("  action:").append(switch (action) {
                case 0 -> "connect";
                case 1 -> "announce";
                case 2 -> "scrape";
                default -> "unknown";
            });
            builder.append("  cid:").append(cid);
        } else {
            if (buffer.remaining() < 8) {
                builder.append(" incorrect length");
                System.out.println(builder.toString());
                return;
            }
            action = buffer.getInt(0);
            tx = buffer.getInt(4);

            builder.append("    tx:").append(tx);
            builder.append("  action:").append(switch (action) {
                case 0 -> "connect";
                case 1 -> "announce";
                case 2 -> "scrape";
                default -> "unknown";
            });
        }
        builder.append("\n");

        if (!outbound && (action == 0)) {
            // received connect
            if (buffer.remaining() != 16) {
                builder.append(" incorrect length");
                System.out.println(builder.toString());
                return;
            }
            long cid = buffer.getLong(8);
            builder.append("  cid:").append(cid);
            builder.append("\n");
        }

        if (outbound && (action == 1)) {
            // announce
            if (buffer.remaining() != 98) {
                builder.append(" incorrect length");
                System.out.println(builder.toString());
                return;
            }
            HashId hash = new HashId();
            HashId peer = new HashId();
            buffer.get(16, hash.getBytes());
            buffer.get(36, peer.getBytes());
            long downloaded = buffer.getLong(56);
            long left = buffer.getLong(64);
            long uploaded = buffer.getLong(72);
            int event = buffer.getInt(80);
            int numWant = buffer.getInt(92);
            int port = buffer.getShort(96) & 0xFFFF;

            builder.append("                ");
            builder.append("  hash:").append(hash.toString());
            builder.append("  peer:").append(peer.toString());
            builder.append("\n");

            builder.append("                ");
            builder.append("  event:").append(event);
            builder.append("  downloaded:").append(downloaded);
            builder.append("  left:").append(left);
            builder.append("  uploaded:").append(uploaded);
            builder.append("\n");

            builder.append("                ");
            builder.append("  numwant:").append(numWant);
            builder.append("  port:").append(port);
            builder.append("\n");
        }

        if (!outbound && (action == 1)) {
            // received announce
            if (buffer.remaining() < 20) {
                builder.append(" incorrect length");
                System.out.println(builder.toString());
                return;
            }
            int interval = buffer.getInt(8);
            int leachers = buffer.getInt(12);
            int seeders = buffer.getInt(16);
            builder.append("                ");
            builder.append("  interval:").append(interval);
            builder.append("  seeders:").append(seeders);
            builder.append("  leechers:").append(leachers);
            builder.append("\n");

            try {
                int addrSize;
                if (address.getAddress() instanceof Inet4Address) {
                    addrSize = 4;
                } else {
                    addrSize = 4;
                }

                byte[] addr = new byte[addrSize];
                int records = buffer.remaining() - 20;
                records /= addrSize;

                for (int i = 0; i < records; i++) {
                    buffer.get(20 + addrSize * i, addr);
                    int port = buffer.getShort(20 + addrSize * (i + 1) - 2) & 0xFFFF;

                    InetSocketAddress pAddr = new InetSocketAddress(InetAddress.getByAddress(addr), port);
                    builder.append("                ");
                    builder.append(pAddr.toString()).append("\n");
                }
            } catch (Exception ignored) {
                // must not happen as we don't resolve addresses
            }
        }

        System.out.println(builder.toString());
    }

}
