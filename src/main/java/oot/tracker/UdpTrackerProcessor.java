package oot.tracker;

import oot.dht.HashId;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;
import java.util.*;
import java.util.function.Consumer;

public class UdpTrackerProcessor {

    public static boolean DEBUG = true;

    /**
     * period to consider received connection id valid
     */
    private final static long UDP_CONNECTION_ID_TIMEOUT = 60 * 1000;
    /**
     * max number of received datagrams to handle per one iteration of update
     */
    private final static int UDP_DATAGRAMS_PER_CYCLE_MAX = 16;
    /**
     * magic id for connect message to udp tracker
     */
    private final static long UDP_PROTOCOL_MAGIC_ID = 0x41727101980L;

    /**
     * udp channel to support udp trackers
     */
    private DatagramChannel channel;
    /**
     * buffer for udp datagrams
     */
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(2048).order(ByteOrder.BIG_ENDIAN);

    private HashId clientId;

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
        UdpTracker tracker;
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
        public Operation(long timeout, UdpTracker tracker, Consumer<Boolean> callback) {
            this.tx = nextTransactionId();
            this.tracker = tracker;
            this.callback = callback;
            this.timeout = timeout;
            this.timestamp = System.currentTimeMillis();
        }

        /**
         * called to update state of the operation
         * @return true if operation must be finished and false otherwise
         * todo: if send fails with buffer overflow, there is no need to call other operations on the same turn,
         * todo: need more detailed result from this method
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

        public UDPConnectOperation(long timeout, UdpTracker tracker, InetSocketAddress address, Consumer<Boolean> callback) {
            super(timeout, tracker, callback);
            this.address = address;
        }

        public UDPConnectOperation(UdpTracker tracker, InetSocketAddress address, Consumer<Boolean> callback) {
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
        private Tracker.AnnounceEvent event;
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
         * ref to callback to be notified with new seeders/leechers found
         */
        private Tracker.AnnounceCallback  peerCallback;

        /**
         * allowed constructor
         * @param timeout operation specific timeout
         * @param tracker tracker to announce state too
         * @param _event announce _event
         * @param _peerCallback optional callback to be notified with new peers
         */
        public UDPAnnounceOperation(long timeout, UdpTracker tracker, Tracker.AnnounceEvent _event, Tracker.AnnounceCallback  _peerCallback)
        {
            super(timeout, tracker, null);
            this.event = _event;
            this.peerCallback = _peerCallback;
        }

        /**
         * allowed constructor,
         * default timeout is used
         * @param tracker tracker to announce state too
         * @param event announce event
         * @param callback optional callback
         */
        public UDPAnnounceOperation(UdpTracker tracker, Tracker.AnnounceEvent event, Tracker.AnnounceCallback  callback) {
            this(OPERATION_TIMEOUT, tracker, event, callback);
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
            buffer.put(tracker.torrent.metainfo.infohash.getBytes());
            buffer.put(clientId.getBytes());
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

            tracker.updatePeriod = buffer.getInt(8);
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
            tracker.updateLastTime = System.currentTimeMillis();

            if (callback != null) {
                callback.accept(true);
            }

            if (peerCallback != null) {
                peerCallback.call(true, peers);
            }

            return OPERATION_FINISH;
        }
    }


    /**
     * collection of active operations, accessed only from
     * the main processing thread
     */
    final List<Operation> operations = new ArrayList<>();
    /**
     * collection to submit new operation from
     * another threads, synchronized
     */
    final List<Operation> newOperations = new ArrayList<>();

    /**
     * current transaction id
     */
    private int tx;

    /**
     * ref to the main service thread
     */
    protected final Thread thread;

    /**
     * ref to callback to be notified with peers discovered,
     * could be registered dynamically
     */
    protected volatile Consumer<Collection<InetSocketAddress>> peersCallback;

    /**
     * allowed constructor
     * @param _clientId id of the client
     */
    public UdpTrackerProcessor(HashId _clientId)
    {
        clientId = _clientId;

        thread = new Thread( this::cycle, "oot-udp-tracker");
        thread.setDaemon(true);
    }

    /**
     * registers callback to be called when peers are discovered
     * via processed trackers
     * @param cb callback
     */
    public void setPeersCallback(Consumer<Collection<InetSocketAddress>> cb) {
        peersCallback =  cb;
    }

    /**
     * main api method to submit announce requests from torrent
     * @param event announce event
     * @param cb callback to be notified
     */
    public void process(UdpTracker tracker, Tracker.AnnounceEvent event, Tracker.AnnounceCallback cb)
    {
        synchronized (newOperations) {
            newOperations.add( new UDPAnnounceOperation(tracker, event, ((success, peers) -> {
                Consumer<Collection<InetSocketAddress>> extCallback = this.peersCallback;
                // notify external callback if registered
                if (extCallback != null) {
                    extCallback.accept(peers);
                }
                if (cb != null) {
                    cb.call(success, peers);
                }
            })));


            newOperations.notify();
        }
    }


    /**
     * @return next transaction id
     */
    protected int nextTransactionId()
    {
        // todo make it random
        tx += 1;
        return tx;
    }

    /**
     * initializes manager
     * @throws IOException if any io error occurred
     */
    public void start() throws IOException
    {
        channel = DatagramChannel.open(StandardProtocolFamily.INET);
        channel.bind(null);
        //InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
        //channel.setOption(StandardSocketOptions.SO_RCVBUF, );
        //channel.setOption(StandardSocketOptions.SO_SNDBUF, );
        channel.configureBlocking(false);

        thread.start();
    }

    /**
     * api method to stop the processor
     */
    public void stop()
    {
        try {
            thread.interrupt();
            thread.join();
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * called by the main thread to clear resources
     */
    protected void _stop()
    {
        // force drop all operations in progress
        operations.clear();
        synchronized (newOperations) {
            newOperations.clear();
        }

        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {}
        }
    }

    /**
     * service method, sends prepared send buffer
     * @param address destination address to send too
     * @return true if datagram was successfully sent and
     * false otherwise (that could be buffer overflow or some io error)
     */
    protected boolean send(InetSocketAddress address) {
        try {
            if (DEBUG) dumpUdpMessage(true, false, buffer, address);

            int sent = channel.send(buffer, address);
            if (sent == 0)
            {
                // there were no space in system send buffer,
                // exit and let retry on the next update turn,
                // this will re-populate the buffer
                return false;
            }

            return true;
        } catch (Exception e) {
            return false;
        }
    }


    /**
     * main cycle of the running thread
     */
    protected void cycle()
    {
        while (true)
        {
            synchronized (newOperations)
            {
                // copy new operations to main queue
                operations.addAll(newOperations);
                newOperations.clear();

                if (operations.size() == 0) {
                    try {
                        // no active operations, wait
                        newOperations.wait(100);
                    } catch (InterruptedException e) {
                        break;
                    }

                    // proceed even if don't have operations,
                    // thi will read from the channel
                }
            }

            boolean error = !update();

            // check state another time
            if (Thread.interrupted()) {
                break;
            }
        }

        stop();
    }

    /**
     * main method, called inside the personal thread
     * to perform state updates
     * @return true if everything is ok and false in case of errors
     */
    protected boolean update()
    {
        long now = System.currentTimeMillis();
        SocketAddress address;
        buffer.clear();

        // read a number of datagrams and
        // link them to active operations
        int datagrams = UDP_DATAGRAMS_PER_CYCLE_MAX;
        while (0 < datagrams--)
        {
            try {
                // channel is in non-blocking mode, so this will not block
                if ((address = channel.receive(buffer)) == null) {
                    // nothing more in the channel
                    break;
                }
            } catch (IOException ignored) {
                return false;
            }

            buffer.flip();
            if (DEBUG) dumpUdpMessage(false, false, buffer, (InetSocketAddress) address);
            if (buffer.remaining() < 8) {
                // incorrect message, minimal
                // number of fields is missing
                continue;
            }

            // peek transaction & send to the linked operation
            int tx = buffer.getInt(4);
            for (int i = 0; i < operations.size(); i++)
            {
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

        // update all active operations
        for (int i = operations.size() - 1; 0 <= i; i--)
        {
            Operation operation =  operations.get(i);
            boolean finished = operation.update();
            if (finished) {
                operations.remove(i);
            }
        }

        return true;
    }

    /**
     * dumps udp message to stdout
     * @param outbound true if message is outgoing
     * @param skipped true if message was not really sent (errors)
     * @param buffer buffer with the message (will not be modified)
     * @param address destination / source address
     */
    private static void dumpUdpMessage(boolean outbound, boolean skipped, ByteBuffer buffer, InetSocketAddress address)
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
