package oot.dht;

import oot.be.BEParser;
import oot.be.BEValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * contains local DHT node information together with
 * necessary service parameters
 */
public class Node {
    /**
     * switch to allow debug messages to stdout
     */
    private static final boolean DEBUG = true;

    /**
     * max number of packets received and processed in one turn
     */
    private final static int RECEIVE_PACKETS_MAX = 16;
    /**
     * time period to run internal cleaning (tokens, downloaders, etc),
     * this is just the cleaning job period.
     */
    private final static long NODE_JOB_CLEANING_TIMEOUT     = 60 * 1000;
    /**
     * time period to check nodes in the routing table for ping timeouts
     */
    private final static long NODE_JOB_PING_ROUTING_TIMEOUT = 15 * 60 * 1000;


    // internal constants with message codes
    private final static byte[] QUERY_PING            = "ping".getBytes(StandardCharsets.UTF_8);
    private final static byte[] QUERY_FIND_NODE       = "find_node".getBytes(StandardCharsets.UTF_8);
    private final static byte[] QUERY_GET_PEERS       = "get_peers".getBytes(StandardCharsets.UTF_8);
    private final static byte[] QUERY_ANNOUNCE_PEER   = "announce_peer".getBytes(StandardCharsets.UTF_8);

    /**
     * background thread for node processing
     */
    private class NodeThread extends Thread
    {
        // update period in ms
        long period;
        // stop flag
        volatile boolean active = true;

        /**
         * allowed constructor
         * @param period update period
         */
        public NodeThread(long period) {
            super("oot.dht.node.thread");
            this.period = period;
        }

        @Override
        public void run()
        {
            while (active) {
                update();
                try {
                    Thread.sleep(period);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    /**
     * unique node id
     */
    HashId id;

    /**
     * routing table with information about all known nodes
     */
    public RoutingTable routing = new RoutingTable(this);

    /**
     * stores peers that announced themselves as downloaders
     * of some torrent(s)
     * Map<torrent hash, Set<node>>
     *
     * todo include local node
     */
    public Map<HashId, Set<RoutingTable.RemoteNode>> peers = new HashMap<>();

    /**
     * stores remote nodes that sent us get_peers request and we've
     * generated token for each of them to check future announce requests,
     * need this as routing table could possible have no space for
     * them or just remove them for some reason (timeout).
     *
     * as tokens are a protection against massive fake announces and
     * are used together with ip address of a node, we generate only one
     * token for each node (not related to the torrent in a request)
     *
     * must be cleared periodically, but the period must be longer
     * then the one used for routing table clearance.

     * map<node id, remote node>
     */
    public Map<HashId, RoutingTable.RemoteNode> tokens = new HashMap<>();

    /**
     * if specified this callback will be called for any announce_peer
     * message received by this node, could be used to notify torrent client
     * with new peers
     * todo make registration method
     */
    public BiConsumer<HashId, InetSocketAddress> callbackAnnounce;

    // indicates if this node has connection
    // to dht network or not, used to delay operations' requests
    private boolean bootstrapped = false;

    // transaction id for a new operation initiated by the local node
    // todo: extend to long (as received from the wild)
    private short transaction;

    // port of the peer that accepts peer-2-peer connections,
    // could be zero if the same as DHT must be used
    private volatile int peerPort;

    // binary encoding parser used across the node
    private final BEParser parser = new BEParser();

    /**
     * channel to send/receive dht messages,
     * could be replaced with channel/selector
     */
    private DatagramChannel channel;

    /**
     * externally added operations, are moved to main
     * collection internally with synchronization
     */
    private final List<Operation> extOperations = new ArrayList<>();

    /**
     * active operations performed by the node,
     * include all types of dht queries
     */
    private final List<Operation> operations = new ArrayList<>();

    /**
     * buffer for packets' receive operations,
     * must have enough space to write the whole datagram
     */
    private final ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(2048);
    /**
     * buffer for outgoing messages,
     * must have enough space to write the whole datagram
     */
    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(1500);

    /**
     * instance of the processing thread
     */
    private NodeThread thread;

    /**
     * last time the cleaning job was activated
     */
    private long timeJobCleaning = 0;
    /**
     * last time the ping routing job was activated
     */
    private long timeJobPingRouting = 0;


    /**
     * @return id of the node
     */
    public HashId getId() {
        return id;
    }

    /**
     * creates node with newly generated id
     */
    public Node() {
        id = generateUniqueId();
    }

    /**
     * creates node with the specific id
     * @param _id node id, this could be global client or something else
     */
    public Node(HashId _id) {
        id = _id;
    }

    /**
     * tries to restore state of the node,
     * including id and routing table,
     * generates new unique id if state can't be parsed
     * @param state serialized state
     */
    public Node(byte[] state) {
        if (!restore(state)) {
            id = generateUniqueId();
        }
    }

    /**
     * sets port of the linked peer to send via announce messages
     * @param peerPort port
     */
    public void setPeerPort(int peerPort) {
        this.peerPort = peerPort;
    }

    /**
     * registerss announce callback to be notified when external announce message is received,
     * callback will be called from the node executing thread
     * @param callbackAnnounce callback
     */
    public void setCallbackAnnounce(BiConsumer<HashId, InetSocketAddress> callbackAnnounce) {
        this.callbackAnnounce = callbackAnnounce;
    }

    /**
     * starts bootstrap operation against the specified nodes
     * to populate the routing table and announce this node
     * @param seed list of initial addresses
     */
    public void bootstrap(List<InetSocketAddress> seed) {
        synchronized (extOperations) {
            extOperations.add(new BootstrapOperation(this, seed, null));
        }
    }

    /**
     * public api to announce information about a torrent
     * being downloaded locally
     * @param hash torrent hash
     */
    public void announce(HashId hash) {
        synchronized (extOperations) {
            extOperations.add(new AnnounceTorrentOperation(this, hash));
        }
    }

    /**
     * runs search for peers that are downloading or stores the specified torrent,
     * running party will be notified via callback called from this node's thread
     * @param hash torrent hash
     * @param callback calback to be notified
     */
    public void findPeers(HashId hash, Consumer<Set<InetSocketAddress>> callback) {
        synchronized (extOperations) {
            extOperations.add(new FindTorrentPeersOperation(this, hash, callback));
        }
    }

    /**
     * runs state collecting operation, callback will be notified
     * in bounds of node's thread
     * @param callback callback to be notified with the state of the node
     */
    public void getState(Consumer<byte[]> callback) {
        synchronized (extOperations) {
            extOperations.add(new GetNodeStateOperations(this, callback));
        }
    }

    /**
     * runs state collecting operation in blocking mode
     */
    public byte[] getStateWithBlocking()
    {
        SynchronousQueue<byte[]> queue = new SynchronousQueue<>();

        // add operation to be executed in nodes' thread
        synchronized (extOperations) {
            extOperations.add(new GetNodeStateOperations(this, queue::offer));
        }

        // wait for data
        byte[] data = null;
        try {
            data = queue.poll(30, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {}
        return data;
    }

    /**
     * starts separate thread to maintain this node
     * and run operations
     */
    public synchronized void start() throws Exception
    {
        if (thread != null) {
            // already started
            return;
        }

        // perform initialization
        reinit();

        // run background service
        thread = new NodeThread(10);
        thread.setDaemon(false);
        thread.start();
    }


    /**
     * stops node and clears resources
     */
    public synchronized void stop()
    {
        try {
            if (thread != null) {
                thread.active = false;
                thread.join();
                thread = null;
            }
        } catch (InterruptedException ignored) {}

        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException ignored) {
        }
    }

    /**
     * tries to generate unique id for this node
     * @return generated id
     */
    private HashId generateUniqueId() {
        try {
            SecureRandom random = SecureRandom.getInstanceStrong();
            return HashId.wrap(random.generateSeed(HashId.HASH_LENGTH_BYTES));
        } catch (NoSuchAlgorithmException e) {
            Random random = new Random();
            random.setSeed(System.nanoTime());
            byte[] tmp = new byte[HashId.HASH_LENGTH_BYTES];
            random.nextBytes(tmp);
            return HashId.wrap(tmp);
        }
    }

    /**
     * performs reinitialization of the node,
     * could be called from a processing thread
     * @throws Exception af any
     */
    private void reinit() throws Exception
    {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {}
        }

        channel = DatagramChannel.open(StandardProtocolFamily.INET);
        channel.bind(null);
        //InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
        //channel.setOption(StandardSocketOptions.SO_RCVBUF, );
        //channel.setOption(StandardSocketOptions.SO_SNDBUF, );
        channel.configureBlocking(false);
    }

    /**
     * generates next transactions id
     * @return next transaction id
     */
    private short nextTransactionId() {
        return transaction++;
    }

    /**
     * adds operation to the list of active operations for execution
     * @param operation operation
     */
    private void addOperation(Operation operation) {
        operation.setTransaction(nextTransactionId());
        operations.add(operation);
    }

    /**
     * incrementally updates node state, receives incoming messages
     * and processes all queries,
     * could be call externally only if node doesn't work
     * inside it's own thread started with {@link #start()} method
     */
    private void update()
    {
        // process incoming messages,
        // this will create more Operations so why
        // we call receive() before operations update
        receive();

        // add external requests for processing
        synchronized (extOperations) {
            extOperations.forEach(this::addOperation);
            extOperations.clear();
        }

        // let all active operations to proceed
        for (int i = operations.size() - 1; 0 <= i; i--) {
            Operation operation = operations.get(i);
            boolean done = operation.update();
            if (done) {
                operations.remove(i);
            }
        }

        long now = System.currentTimeMillis();

        if (timeJobCleaning + NODE_JOB_CLEANING_TIMEOUT < now) {
            clean();
            timeJobCleaning = now;
        }

        if (timeJobPingRouting + NODE_JOB_PING_ROUTING_TIMEOUT < now) {
            addOperation(new PingNodesInRoutingTableOperation(this));
            timeJobPingRouting = now;
        }
    }

    /**
     * performs internal cleaning, removes
     * outdated tokens and announced downloaders
     */
    private void clean()
    {
        // drop outdated tokens
        Iterator<Map.Entry<HashId, RoutingTable.RemoteNode>> tIterator = tokens.entrySet().iterator();
        while (tIterator.hasNext()) {
            Map.Entry<HashId, RoutingTable.RemoteNode> next = tIterator.next();
            RoutingTable.RemoteNode rNode = next.getValue();
            if (rNode.isTokenLocalExpired()) {
                tIterator.remove();
            }
        }

        // drop outdated announcers
        Iterator<Map.Entry<HashId, Set<RoutingTable.RemoteNode>>> pIterator = peers.entrySet().iterator();
        while (pIterator.hasNext()) {
            Map.Entry<HashId, Set<RoutingTable.RemoteNode>> next = pIterator.next();
            Set<RoutingTable.RemoteNode> rNodes = next.getValue();
            rNodes.removeIf(RoutingTable.RemoteNode::isAnnounceExternalExpired);
            if (rNodes.isEmpty()) {
                pIterator.remove();
            }
        }
    }


    /**
     * processes all received packets from the node's channel,
     * packets could be responses for the previous queries or
     * queries from external nodes.
     * @link http://bittorrent.org/beps/bep_0005.html
     *
     * query     {"t":"aa", "y":"q", "q":name "a":{parameters dictionary}}
     * response  {"t":"aa", "y":"r", "r":{response dictionary}}
     * error     {"t":"aa", "y":"e", "e":[201, "A Generic Error Ocurred"]}
     *
     * all queries and responses contain "id" with id of the node that sends query/response
     *
     * queries:
     * ping         {"t":"aa", "y":"r", "r": {"id":"mnopqrstuvwxyz123456"}}
     * find_node    {"t":"aa", "y":"r", "r": {"id":"0123456789abcdefghij", "nodes": "def456..."}}
     * get_peers    {"t":"aa", "y":"r", "r": {"id":"abcdefghij0123456789", "token":"aoeusnth", "values": ["axje.u", "idhtnm"]}}
     *              {"t":"aa", "y":"r", "r": {"id":"abcdefghij0123456789", "token":"aoeusnth", "nodes": "def456..."}}
     *announce_peer {"t":"aa", "y":"r", "r": {"id":"mnopqrstuvwxyz123456"}}
     */
    private void receive()
    {
        // alias
        ByteBuffer buffer = receiveBuffer;

        // counter of received packets
        // used to check for max allowed packets received in one turn
        int recvCounter = 0;

        do {
            // protection from a massive incoming stream,
            // they will be handled on the next turn
            if (RECEIVE_PACKETS_MAX < recvCounter) {
                return;
            }

            // channel is in non-blocking mode, so this will not block
            SocketAddress address;
            try {
                buffer.clear(); // clear buffer to be available for writing
                if ((address = channel.receive(buffer)) == null) {
                    return;     // nothing in the channel
                }
            } catch (Exception e) {
                if (DEBUG) System.out.println(e.getMessage());
                // try to recover with re-init
                // and leave till the next turn
                try {
                    reinit();
                } catch (Exception ignored) {
                }
                return;
            }

            recvCounter++;

            // prepare buffer to read the received data
            buffer.flip();

            BEValue parsed;
            try {
                parsed = parser.parse(buffer);
            } catch (Exception e) {
                if (DEBUG) System.out.println("incorrect message format: " + e);
                // leave till the next turn
                return;
            }

            if (DEBUG) {
                ProtocolDumper.dumpMessage(false, false, address, parsed);
            }

            // is this a response message?
            BEValue y = parsed.dictionary.get("y");
            if (y.equals('r')) {
                processExternalResponse(address, parsed);
                continue;
            }

            // check if this is a query message from some external node
            if (y.equals('q')) {
                processExternalQuery(address, parsed);
                continue;
            }

            // check if this is an error received from external node,
            // there could be a problem if external node doesn't reply,
            // but sends error message - that could drop some
            // our transaction that is in progress...
            // also nodes could respond with error if they don't know
            // requested id, for example:  [203  Invalid `id' value]
            if (y.equals('e')) {
                processExternalError(address, parsed);
                continue;
            }

            // log incorrect message type
            if (DEBUG) {
                System.out.println("incorrect reply type");
            }

        } while (true);
    }

    /**
     * service method, sends prepared send buffer
     * @param address destination address to send too
     * @return true if datagram was successfully sent and
     * false otherwise (that could be buffer overflow or some io error)
     */
    private boolean send(InetSocketAddress address) {
        try {
            BEValue message = null;

            if (DEBUG) {
                try {
                    message = parser.parse(sendBuffer);
                    sendBuffer.rewind();
                } catch (Exception ignored) {}
            }

            int sent = channel.send(sendBuffer, address);
            if (sent == 0)
            {
                if (DEBUG) ProtocolDumper.dumpMessage(true, true, address, message);

                // there were no space in system send buffer,
                // exit and let retry on the next update turn
                return false;
            }

            if (DEBUG) ProtocolDumper.dumpMessage(true, false, address, message);

            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * called to process received error message from an external node
     * @param address address of the node
     * @param data parsed response
     */
    private void processExternalError(SocketAddress address, BEValue data) {
        BEValue tx = data.dictionary.get("t");
        BEValue error = data.dictionary.get("e");

        // find by tx and inform about error
        for (int i = 0; i < operations.size(); i++) {
            Operation operation = operations.get(i);
            if ( !operation.cmpTransaction(tx.bString) ) {
                continue;
            }

            if (operation.error((InetSocketAddress) address, error)) {
                // operation indicates it's over, remove it
                operations.remove(i);
            }
            break;
        }
    }

    /**
     * called to process received response message
     * @param address address of the external node
     * @param data parsed response message
     */
    private void processExternalResponse(SocketAddress address, BEValue data) {
        //BEValue version = data.dictionary.get("v");
        BEValue transaction_id = data.dictionary.get("t");
        BEValue response = data.dictionary.get("r");

        boolean txFound = false;
        // find by tx and send to process
        for (int i = 0; i < operations.size(); i++) {
            Operation operation = operations.get(i);
            if ( !operation.cmpTransaction(transaction_id.bString) ) {
                continue;
            }
            txFound = true;
            if (operation.receive((InetSocketAddress) address, response)) {
                // operation indicates it's over, remove it
                operations.remove(i);
            }
            break;
        }
        if (DEBUG && !txFound) {
            // log possible error
            System.out.println("reply message: tx without operation");
        }
    }




    /**
     * called when external request received
     * @param address address of the requester
     * @param query query to be executed
     */
    private void processExternalQuery(SocketAddress address, BEValue query) {
        if (!BEValue.isDictNotEmpty(query)) {
            if (DEBUG) System.out.println("externalQuery: query is empty");
            return;
        }

        BEValue q = query.dictionary.get("q");
        if (!BEValue.isBStringNotEmpty(q)) {
            if (DEBUG) System.out.println("externalQuery: 'q' is missing");
            return;
        }

        if (!(address instanceof InetSocketAddress)) {
            if (DEBUG) System.out.println("externalQuery: address is a ISA");
            return;
        }

        if (Arrays.equals(QUERY_PING, q.bString)) {
            addOperation(new PingResponseOperation(this, (InetSocketAddress) address, query));
        }
        else if (Arrays.equals(QUERY_FIND_NODE, q.bString)) {
            addOperation(new FinNodeResponseOperation(this, (InetSocketAddress) address, query));
        }
        else if (Arrays.equals(QUERY_GET_PEERS, q.bString)) {
            addOperation(new GetPeersResponseOperation(this, (InetSocketAddress) address, query));
        }
        else if (Arrays.equals(QUERY_ANNOUNCE_PEER, q.bString)) {
            addOperation(new AnnounceResponseOperation(this, (InetSocketAddress) address, query));
        } else {
            if (DEBUG) System.out.println("externalQuery: unknown operation");
        }
    }

    /**
     * parses binary representation of nodes list stored in compact form,
     * this could be moved to utility class
     * @param binary binary format
     * @return not null list of parsed nodes
     */
    List<RoutingTable.RemoteNode> parseNodesCompactForm(byte[] binary)
    {
        // each compact form is always 26 bytes
        int count = binary.length / 26;
        List<RoutingTable.RemoteNode> nodes = new ArrayList<>(count);

        for (int i = 0; i < count; i++)
        {
            int index = i * 26;
            HashId id = new HashId(binary, index);

            // extract node address as ipv4
            index += HashId.HASH_LENGTH_BYTES;
            byte[] addr = Arrays.copyOfRange(binary, index, index + 4);

            // extract port as short
            index += 4;
            int port = ((Byte.toUnsignedInt(binary[index]) << 8) +
                    Byte.toUnsignedInt(binary[index + 1]));

            InetSocketAddress address;
            try {
                address = new InetSocketAddress(Inet4Address.getByAddress(addr), port);
            } catch (UnknownHostException e) {
                // that shouldn't be as we don't resolve address
                if (DEBUG) System.out.println("parseNodesCompactForm: " + e.getMessage());
                throw new RuntimeException(e);
            }

            RoutingTable.RemoteNode tmp = routing.new RemoteNode(id, address);
            nodes.add(tmp);
        }
        return nodes;
    }

    /**
     * parses binary ip4 address format, 4 bytes address and 2 bytes port as big-endian,
     * this could be moved to utility class
     * @param binary array with data, must be of length 6
     * @return parsed address or null if something missing or wrong
     */
    InetSocketAddress parsePeerCompactForm(byte[] binary)
    {
        if ((binary == null) || (binary.length != 6)) {
            return null;
        }

        byte[] addr = Arrays.copyOfRange(binary, 0, 4);
        int port = ((Byte.toUnsignedInt(binary[4]) << 8) +
                Byte.toUnsignedInt(binary[5]));

        try {
            return new InetSocketAddress(Inet4Address.getByAddress(addr), port);
        } catch (UnknownHostException e) {
            // that shouldn't be as we don't resolve address
            if (DEBUG) System.out.println("parsePeerCompactForm: " + e.getMessage());
            return null;
        }
    }


    /**
     * @return array with encoded internal state of the routing table,
     * could be later used to restore state and not start from scratch.
     * format:
     * [#magic][version][node id][#nodes][node]..[node]
     *
     * node:
     * [hash] [addr_type] [addr]?
     *
     * TODO include tokens/peers
     */
    private byte[] getState()
    {
        List<RoutingTable.RemoteNode> rNodes = routing.getRemoteNodes();

        int estimate = 8 + 20 + 2 + rNodes.size() * 27;
        ByteArrayOutputStream os = new ByteArrayOutputStream(estimate);

        // magic number
        os.writeBytes(new byte[] {'o', 'o', 'r', 't'});
        // reserved + version
        os.writeBytes(new byte[] {0, 0, 0, 1});
        // node id
        os.writeBytes(id.getBytes());

        // number of nodes
        int nodes = rNodes.size();
        os.write((nodes >> 8) & 0xFF);
        os.write(nodes & 0xFF);


        for (RoutingTable.RemoteNode rn : rNodes)
        {
            // node hash
            os.writeBytes(rn.id.getBytes());

            InetAddress address = rn.address.getAddress();
            if (address instanceof Inet4Address) {
                // address type and address
                os.write(1);
                os.writeBytes(address.getAddress());
            }
            else if (address instanceof Inet6Address) {
                // address type and address
                os.write(2);
                os.writeBytes(address.getAddress());
            } else {
                // address type unknown, no address data
                os.write(-1);
            }
        }

        return os.toByteArray();
    }

    /**
     * restores internal state of the routing table
     * @param state previously encoded internal state
     */
    private boolean restore(byte[] state)
    {
        try {
            boolean success = _restore(state);
            if (!success) {
                System.out.println("RoutingTable: incorrect state (some errors), can't restore");
            }
            return success;
        } catch (Exception ignored) {
            System.out.println("RoutingTable: incorrect state (too short), can't restore");
            return false;
        }
    }

    /**
     * restores internal state of the routing table
     * @param state previously encoded internal state
     */
    private boolean _restore(byte[] state) throws Exception
    {
        ByteBuffer buffer = ByteBuffer.wrap(state);
        buffer.order(ByteOrder.BIG_ENDIAN);


        byte[] tmp = new byte[4];
        buffer.get(tmp);
        if (!Arrays.equals(tmp, new byte[] {'o', 'o', 'r', 't'})) {
            return false;
        }

        // skip upper part of reserved/version
        buffer.position(buffer.position() + 3);

        byte version = buffer.get();
        if (version != 1) {
            return false;
        }

        tmp = new byte[HashId.HASH_LENGTH_BYTES];
        buffer.get(tmp);
        HashId nodeId = HashId.wrap(tmp);

        int nodes = buffer.getShort();

        // make new root, to not break the current one
        RoutingTable rTable = new RoutingTable(this);
        for (int i = 0; i < nodes; i++)
        {

            // node hash
            tmp = new byte[HashId.HASH_LENGTH_BYTES];
            buffer.get(tmp);
            HashId hash = HashId.wrap(tmp);

            //address type
            InetAddress iAddr;
            int port = -1;
            byte type = buffer.get();
            if (type == 1) {
                tmp = new byte[4];
                buffer.get(tmp);
                iAddr = Inet4Address.getByAddress(tmp);
                port = buffer.getShort();
            } else if (type == 2) {
                tmp = new byte[16];
                buffer.get(tmp);
                iAddr = Inet6Address.getByAddress(tmp);
                port = buffer.getShort();
            } else if (type == -1) {
                // node without address, skip
                iAddr = null;
            } else {
                // unknown, exit
                return false;
            }

            if ((iAddr != null) && (port != -1)) {
                InetSocketAddress address = new InetSocketAddress(iAddr, port);
                rTable.insert(hash, address);
            }
        }

        // apply
        id = nodeId;
        routing = rTable;

        return true;
    }


    /**
     * Base class for all operations that run inside the local node,
     * includes simple protocol operation, complex operations on top of protocol messages,
     * possible utility/maintenance operations and operations, spawned on received messages
     *
     * Provides non blocking way to perform various tasks inside a node.
     */
    private abstract static class Operation {

        // alias for internal usage, indicates operation must be finished
        static final boolean OPERATION_FINISH = true;

        // alias for internal usage, indicates operation must be proceeded with execution
        static final boolean OPERATION_RERUN = false;

        // max allowed number of errors for an operation to stop
        static final int MAX_SEND_ERRORS = 2;

        // default timeout for an operation to stop, in milliseconds
        static final long DEFAULT_OPERATION_TIMEOUT = 4_000L;

        // transaction id of the operation,
        // could be modified during several-step-operations,
        // need array as external transactions could send something long,
        // usually populated when operation is added for execution
        byte[] transaction;

        // start time of the operation to track timeouts
        long timestamp;
        // timeout could be different for various operations
        public long timeout;

        // ref to local node
        Node node;

        // counts numbers of datagrams sent,
        // need this in case if we can't send all datagrams in
        // one 'update' turn
        int sentCounter = 0;

        // counts number of received datagrams for this operation
        int recvCounter = 0;

        // tracks number of errors to end the operation
        int errCounter = 0;

        /**
         * generic constructor
         * @param _timestamp start time of the operation
         * @param _timeout timeout of the operation
         */
        Operation(Node _node, long _timestamp, long _timeout) {
            node = _node;
            timestamp = _timestamp;
            timeout = _timeout;
        }

        /**
         * default timings constructor,
         * uses current timestamp as start time and DEF_TIMEOUT as timeout
         */
        Operation(Node _node) {
            this(_node, System.currentTimeMillis(), DEFAULT_OPERATION_TIMEOUT);
        }

        /**
         * processes any response from the outer space,
         * it's a good idea to check external address for validness
         * with transaction id of this operation
         * @param address address of the sending party
         * @param response response received, only the internal response element
         * @return must return true if operation must be discarded after processing
         */
        boolean receive(InetSocketAddress address, BEValue response) {
            return OPERATION_RERUN;
        }

        /**
         * called to notify operation about error received in bounds of it's transaction,
         * it's a good idea to check address too
         * @param address address of the sending party
         * @param response response received, only the error element
         * @return must return true if operation must be discarded after processing
         */
        boolean error(InetSocketAddress address, BEValue response) {
            return OPERATION_RERUN;
        }

        /**
         * update operation's state, called periodically
         * to track timeouts and proceed with steps inside the operation,
         * each operation is allowed to exclusively use send buffer during this call
         * @return must return true if operation must be discarded after processing
         */
        boolean update() {
            return OPERATION_RERUN;
        }

        /**
         * service method, sends prepared send buffer with {@link Node#send(InetSocketAddress)}
         * and tracks local send/error counters
         * @param address destination address to send too
         * @return true if datagram was successfully sent and
         * false otherwise (that could be buffer overflow or some io error)
         */
        protected boolean send(InetSocketAddress address) {
            boolean result = node.send(address);
            if (result) {
                // ok, track successful messages
                sentCounter++;
            } else {
                // something happened, track number of errors
                errCounter++;
            }
            return result;
        }


        /**
         * sets id ot his transaction
         * @param id id to be set
         */
        void setTransaction(short id) {
            if ((transaction == null) || (transaction.length != 2)) {
                transaction = new byte[2];
            }
            transaction[0] = (byte)((id >> 8) & 0xFF);
            transaction[1] = (byte)(id & 0xFF);
        }

        /**
         * sets id ot this transaction
         * @param id id to be set
         */
        void setTransaction(byte[] id) {
            transaction = id;
        }

        /**
         * tests if this operation's transaction id is the same as the specified one
         * @param id transaction id to check agains local one
         * @return true if identical
         */
        boolean cmpTransaction(byte[] id) {
            return Arrays.equals(transaction, id);
        }
    }


    /**
     * base class for ping operations that check remote peers and/or finds their ids
     * in bounds of one operation
     */
    private static abstract class PingOperation extends Operation {

        /**
         * template for outgoing messages,
         * 2 bytes transaction id and 20 bytes hash id must be substituted
         */
        static byte[] TEMPLATE = "d1:y1:q1:q4:ping1:t2:TX1:ad2:id20:XXXXXXXXXXXXXXXXXXXXee"
                .getBytes(StandardCharsets.UTF_8);

        /**
         * position of transaction id in the template
         */
        static final int TEMPLATE_POS_TX = 21;
        /**
         * position of hash id in the template
         */
        static final int TEMPLATE_POS_HASH = 34;


        /**
         * allowed constructor
         * @param _node ref to local node
         */
        public PingOperation(Node _node) {
            super(_node);
        }

        /**
         * populates the specified buffer with the message
         * review:
         * remove extra copy of the id part, #TEMPLATE could be updated
         * in constructor as node is must be stable
         * @param buffer buffer to populate
         * @param tx transaction id, must be 2 bytes array
         */
        void populateSendBuffer(ByteBuffer buffer, byte[] tx) {
            buffer.clear();
            buffer.put(TEMPLATE);
            buffer.put(TEMPLATE_POS_TX, tx[0]).put(TEMPLATE_POS_TX + 1, tx[1]);
            buffer.put(TEMPLATE_POS_HASH, node.id.getBytes());
            buffer.flip();
        }
    }

    /**
     * ping operation checks remote peers and finds their ids,
     * intended for bootstrapping procedures/adding new nodes into routing table
     * so always tries to insert answered nodes into the routing table.
     *
     * returns only answered nodes to the calling party,
     * doesn't try to remove not answered peers from routing table
     *
     * uses the same transaction id for ping requests to different addresses
     * in bounds of one operation
     */
    public static class PingPeersOperation extends PingOperation {

        // peers to send ping queries too
        private final List<InetSocketAddress> peers;

        // do we need to insert answered nodes into routing or update them if already exist
        private final boolean updateRouting;

        // optional consumer to be notified at the end of the operation
        private final Consumer<List<RoutingTable.RemoteNode>> callback;

        // collection of answered nodes, will be sent to callback consumer
        private List<RoutingTable.RemoteNode> cbNodes;

        /**
         * allowed constructor for a case of multi peers
         * @param _node required ref to local node
         * @param _peers list of peers to ping
         * @param _updateRouting will try to insert answered nodes into routing table
         *                       or update them if already exist
         * @param _callback [optional] will be called at the end of the operation is specified
         */
        public PingPeersOperation(
                Node _node,
                List<InetSocketAddress> _peers,
                boolean _updateRouting,
                Consumer<List<RoutingTable.RemoteNode>> _callback)
        {
            super(_node);
            peers = _peers;
            updateRouting = _updateRouting;

            callback = _callback;
            if (callback != null) {
                cbNodes = new ArrayList<>(peers.size());
            }
        }

        /**
         * allowed constructor for a case of single peer
         * @param _node required ref to local node
         * @param _peer peer to ping
         * @param _updateRouting will try to insert answered node into routing or update it if already exist
         * @param _callback [optional] will be called at the end of the operation is specified
         */
        public PingPeersOperation(
                Node _node,
                InetSocketAddress _peer,
                boolean _updateRouting,
                Consumer<List<RoutingTable.RemoteNode>> _callback)
        {
            this(_node, List.of(_peer), _updateRouting, _callback);
        }

        @Override
        boolean update()
        {
            if (Operation.MAX_SEND_ERRORS < errCounter) {
                // finish ping operation due to errors
                if (callback != null) {
                    callback.accept(cbNodes);
                }
                return OPERATION_FINISH;
            }

            if ((recvCounter == peers.size())
                    || (timestamp + timeout < System.currentTimeMillis()))
            {
                // all responses received or operation timeout reached,
                // "all" check duplicates the same one in receive
                if (callback != null) {
                    callback.accept(cbNodes);
                }
                return OPERATION_FINISH;
            }

            // we use the same tx number and other parameters for all ext nodes
            populateSendBuffer(node.sendBuffer, transaction);

            while (sentCounter < peers.size()) {
                // prepare buffer for re-read
                node.sendBuffer.rewind();

                // send buffer and update counters
                boolean isSent = send(peers.get(sentCounter));
                if (!isSent) {
                    // something happened, indicate the operation needs re-run
                    return OPERATION_RERUN;
                }
            }

            // always indicate operation is still alive,
            // it could be finished on timeout or on 'all received'
            return OPERATION_RERUN;
        }

        @Override
        boolean receive(InetSocketAddress address, BEValue response)
        {
            // count answered peers
            recvCounter++;

            // ping response format
            // {"t":"aa", "y":"r", "r": {"id":"mnopqrstuvwxyz123456"}}
            BEValue beId = response.dictionary.get("id");

            //int index;
            if (updateRouting
                    // && ((index = peers.indexOf(address)) != -1)
                    && (beId != null)
                    && HashId.validate(beId.bString))
            {
                HashId id = HashId.wrap(beId.bString);

                // find node in routing tree
                RoutingTable.RemoteNode rNode = node.routing.getRemoteNode(id);
                if (rNode != null) {
                    // ok, we already know it, update state
                    rNode.updateLastActivityTime();
                } else {
                    // it's missing, try to add
                    rNode = node.routing.new RemoteNode(id, address);
                    rNode = node.routing.insert(rNode);
                }

                if ((cbNodes != null) && (rNode != null)) {
                    cbNodes.add(rNode);
                }
            }

            if (peers.size() <= recvCounter) {
                // don't wait till update, finish operation right now
                if (callback != null) {
                    callback.accept(cbNodes);
                }
                return OPERATION_FINISH;
            } else {
                return OPERATION_RERUN;
            }
        }
    }

    /**
     * meta ping operation that checks nodes from the routing table,
     * operation always updates nodes in routing table with new status,
     * removes nodes if they don't respond.
     *
     * generates {@link PingPeersOperation} operations for each existing level
     * of the routing table
     *
     * uses the same transaction id for ping requests to different addresses
     * in bounds of one operation
     */
    public static class PingNodesInRoutingTableOperation extends PingOperation {
        /**
         * default timeout for this operation,
         * as it performs many requests it usually takes more time
         */
        private final static long PING_ROUTING_NODES_TIMEOUT = 20_000L;

        /**
         * ref to the bucket of the routing table being processed
         */
        private RoutingTable.Bucket current;
        // debug: level or the routing being processed
        private int bucketLevel;

        /**
         * indicator to check if child operations are finished
         */
        boolean callbackCalled;

        /**
         * creates operation with the default timeout ({@link #PING_ROUTING_NODES_TIMEOUT})
         * @param _node ref to local node
         */
        public PingNodesInRoutingTableOperation(Node _node) {
            this(_node, PING_ROUTING_NODES_TIMEOUT);
        }

        /**
         * creates operation
         * @param _node ref to local node
         * @param _timeout specific timeout
         */
        public PingNodesInRoutingTableOperation(Node _node, long _timeout)
        {
            super(_node);

            current = node.routing.root;
            bucketLevel = 0;

            timeout = _timeout;

            // this starts the process
            callbackCalled = true;
        }

        @Override
        boolean update()
        {
            // check if there are no more levels in routing
            if (current == null) {
                return OPERATION_FINISH;
            }

            // global timeout protection
            if (timestamp + timeout < System.currentTimeMillis()) {
                return OPERATION_FINISH;
            }


            // wait while current level is being processed
            if (!callbackCalled) {
                return OPERATION_RERUN;
            }

            // get nodes that are ready to be refreshed
            List<InetSocketAddress> addresses = current.elements.stream()
                    .filter(RoutingTable.RemoteNode::isReadyForPing)
                    .map(rn -> rn.address)
                    .collect(Collectors.toList());

            if (DEBUG) {
                System.out.println("PingNodesInRoutingTable: level: " + bucketLevel + " , to ping: " + addresses.size());
            }

            if (addresses.isEmpty()) {
                current = current.child;
                bucketLevel++;
                return OPERATION_RERUN;
            }

            // reset processing flag and call operation
            callbackCalled = false;
            node.addOperation(new PingPeersOperation(node, addresses, true, ignored -> {
                callbackCalled = true;
                // remove possible dead nodes that were not updated
                current.removeDead();
                // switch to the next level
                current = current.child;
            }));

            return OPERATION_RERUN;
        }

        @Override
        boolean receive(InetSocketAddress address, BEValue response) {
            return OPERATION_RERUN;
        }
    }


    /**
     * runs find_node query against the list of specific nodes.
     * note: it's not the generic "find_node" that queries only one node!
     *
     * this operation is used only during bootstrap to iteratively find
     * closest nodes to the local one, so this operation usually spawned
     * by some other local operation
     *
     * operation returns not more than {@link RoutingTable.Bucket#K} closest elements
     * in the result list (via callback)
     *
     * arguments:  {"id" : "<querying nodes id>", "target" : "<id of target node>"}
     * response: {"id" : "<queried nodes id>", "nodes" : "<compact node info>"}
     */
    private static class FindNodeOperation extends Operation {
        /**
         * template of find_node message with 3 parameters to be set: transaction id,
         * local node id and target hash id to find
         */
        private final static byte[] TEMPLATE =
                "d1:t2:TX1:y1:q1:q9:find_node1:ad2:id20:XXXXXXXXXXXXXXXXXXXX6:target20:XXXXXXXXXXXXXXXXXXXXee"
                        .getBytes(StandardCharsets.UTF_8);

        /**
         * position of transaction id in the template
         */
        private static final int TEMPLATE_POS_TX        = 6;
        /**
         * position of node id in the template
         */
        private static final int TEMPLATE_POS_ID        = 39;
        /**
         * position of target hash in the template
         */
        private static final int TEMPLATE_POS_TARGET    = 70;

        // target hash we are looking for
        HashId target;

        // nodes that will be queries
        List<RoutingTable.RemoteNode> nodes;

        // collection of the result nodes, merged from
        // response received from nodes
        List<RoutingTable.RemoteNode> result;

        // utility array with distances to nodes in response
        HashId[] resultDistances;

        // callback to be called on finish
        Consumer<List<RoutingTable.RemoteNode>> callback;

        /**
         * allowed constructor
         * @param _node ref to local node
         * @param _nodes list of nodes to send queries too
         * @param _target target id we are looking for
         * @param _callback callback to send results too
         */
        public FindNodeOperation(
                Node _node,
                List<RoutingTable.RemoteNode> _nodes,
                HashId _target,
                Consumer<List<RoutingTable.RemoteNode>> _callback)
        {
            super(_node);
            target = _target;
            callback = _callback;

            nodes = _nodes;

            result = new ArrayList<>(RoutingTable.Bucket.K);
            resultDistances = new HashId[RoutingTable.Bucket.K];
        }

        /**
         * populates send buffer with operation message
         * review: remove extra copy of the id part
         * @param buffer buffer to populate
         * @param tx transaction id, only 2 low bytes will be used
         * @param target target node id to use in message
         */
        void populateSendBuffer(ByteBuffer buffer, byte[] tx, HashId target) {
            buffer.clear();
            buffer.put(TEMPLATE);
            buffer.put(TEMPLATE_POS_TX, tx[0]).put(TEMPLATE_POS_TX + 1, tx[1]);
            buffer.put(TEMPLATE_POS_ID, node.id.getBytes());
            buffer.put(TEMPLATE_POS_TARGET, target.getBytes());
            buffer.flip();
        }

        @Override
        boolean update()
        {
            if ((recvCounter == nodes.size()) ||
                    (Operation.MAX_SEND_ERRORS < errCounter) ||
                    (timestamp + timeout < System.currentTimeMillis()))
            {
                if (callback != null) {
                    callback.accept(result);
                }
                // indicate operation is over
                return OPERATION_FINISH;
            }

            // we use the same tx number, current node id is the same too
            populateSendBuffer(node.sendBuffer, transaction, target);
            while (sentCounter < nodes.size()) {
                // prepare buffer for re-read
                node.sendBuffer.rewind();

                boolean isSent = send(nodes.get(sentCounter).address);
                if (!isSent) {
                    // indicate the operation needs a re-run
                    return OPERATION_RERUN;
                }
            }

            // indicate operation is still alive
            return OPERATION_RERUN;
        }


        @Override
        boolean receive(InetSocketAddress address, BEValue response)
        {
            recvCounter++;

            BEValue beQueried = response.dictionary.get("id");
            BEValue beNodes = response.dictionary.get("nodes");

            // that is ok for external nodes to simply drop nodes element
            if (BEValue.isBString(beNodes) && (beNodes.bString != null)) {
                List<RoutingTable.RemoteNode> remoteNodes = node.parseNodesCompactForm(beNodes.bString);
                // build closest list
                for (RoutingTable.RemoteNode rn: remoteNodes) {
                    node.routing.populateClosestList(target,  result, resultDistances, rn);
                }
            }

            // don't wait till check in update if we've received all responses
            if (recvCounter == nodes.size()) {
                if (callback != null) {
                    callback.accept(result);
                }
                return OPERATION_FINISH;
            }

            return OPERATION_RERUN;
        }

        /**
         * simply increments receive counter to end operation
         * when all responses are received, this greatly helps
         * during debug with long timeouts
         * @param address address of the sending party
         * @param response response received, only the error element
         * @return true if operation must be finished
         */
        @Override
        boolean error(InetSocketAddress address, BEValue response) {
            recvCounter++;
            // don't wait till check in update if we've received all responses
            if (recvCounter == nodes.size()) {
                if (callback != null) {
                    callback.accept(result);
                }
                return OPERATION_FINISH;
            }
            return OPERATION_RERUN;
        }
    }


    /**
     * runs get_peers operation against the list of specific nodes.
     * this is an extension of the generic get_peers query that supports
     * querying of several nodes inside one operation
     *
     * NOTES:
     * - original nodes (to be queried) are updated with {@link oot.dht.RoutingTable.RemoteNode#token}
     *   received in reply messages from them
     * - original nodes are inserted into the routing table if correct response is received,
     *   this inserts them or updates them in the routing table
     * - nodes received in response messages are NOT inserted (or checked against) into the routing table
     *   as their status is unknown yet (at least for some of them)
     *
     * callback will be notified with collections that could contain
     * duplicate elements (nodes and peers), this will not happen
     * if only one node is queried (generic get_peers)
     */
    private static class GetPeersOperation extends Operation {
        /**
         * message template with transaction id, local node id and target hash parameters
         */
        private final static byte[] TEMPLATE =
                "d1:t2:TX1:y1:q1:q9:get_peers1:ad2:id20:XXXXXXXXXXXXXXXXXXXX9:info_hash20:XXXXXXXXXXXXXXXXXXXXee".
                getBytes(StandardCharsets.UTF_8);

        /**
         * position of transaction id in the template
         */
        private static final int TEMPLATE_POS_TX = 6;
        /**
         * position of node id in the template
         */
        private static final int TEMPLATE_POS_ID = 39;
        /**
         * position of hash in the template
         */
        private static final int TEMPLATE_POS_HASH = 73;

        // target hash to query for
        HashId target;

        // list of peer found
        List<InetSocketAddress> foundPeers;
        // list of nodes that could be queried further
        List<RoutingTable.RemoteNode> foundNodes;

        // nodes to query for peers
        List<RoutingTable.RemoteNode> nodes;

        // call back to notify on finish
        BiConsumer<List<RoutingTable.RemoteNode>, List<InetSocketAddress>> callback;

        /**
         * allowed constructor
         * @param _node ref to local node
         * @param _nodes list of nodes to query
         * @param _target target hash to search peers by
         * @param _callback callback to notify on finish
         */
        public GetPeersOperation(
                Node _node,
                List<RoutingTable.RemoteNode> _nodes, HashId _target,
                BiConsumer<List<RoutingTable.RemoteNode>, List<InetSocketAddress>> _callback)
        {
            super(_node);
            target = _target;
            nodes = _nodes;
            callback = _callback;

            foundPeers = new ArrayList<>();
            foundNodes = new ArrayList<>();
        }

        /**
         * review: remove extra copy of the id part
         * @param buffer buffer to populate
         * @param tx transaction id, only 2 low bytes will be used
         * @param target target node id to use in message
         */
        void populateSendBuffer(ByteBuffer buffer, byte[] tx, HashId target) {
            buffer.clear();
            buffer.put(TEMPLATE);
            buffer.put(TEMPLATE_POS_TX, tx[0]).put(TEMPLATE_POS_TX + 1, tx[1]);
            buffer.put(TEMPLATE_POS_ID, node.id.getBytes());
            buffer.put(TEMPLATE_POS_HASH, target.getBytes());
            buffer.flip();
        }

        @Override
        boolean update()
        {
            if ((recvCounter == nodes.size()) ||
                    (Operation.MAX_SEND_ERRORS < errCounter) ||
                    (timestamp + timeout < System.currentTimeMillis()))
            {
                if (callback != null) {
                    callback.accept(foundNodes, foundPeers);
                }
                // indicate operation is over
                return OPERATION_FINISH;
            }

            // we use the same tx number, current node id is the same too
            populateSendBuffer(node.sendBuffer, transaction, target);
            while (sentCounter < nodes.size()) {
                // prepare buffer for re-read
                node.sendBuffer.rewind();

                boolean isSent = send(nodes.get(sentCounter).address);
                if (!isSent) {
                    // indicate the operation needs a re-run
                    return false;
                }
            }

            // check here to not wait till next run of the update()
            if (recvCounter == nodes.size()) {
                if (callback != null) {
                    callback.accept(foundNodes, foundPeers);
                }
                // indicate operation is over
                return OPERATION_FINISH;
            }

            // indicate operation is still alive
            return OPERATION_RERUN;
        }


        @Override
        boolean receive(InetSocketAddress address, BEValue response)
        {
            /*
             * response could return peers or nodes to query further,
             * formats are the following (by the spec):
             *   response: {"id" : "<queried nodes id>", "token" :"<opaque write token>", "values" : ["<peer 1 info string>", "<peer 2 info string>"]}
             *         or: {"id" : "<queried nodes id>", "token" :"<opaque write token>", "nodes" : "<compact node info>"}
             *
             * in the real life there are replies that:
             * - have 'nodes' AND 'values' together
             * - have 'values' that is empty list (list with zero elements)
             * - have 'values' with a number of the same elements (even like 5 elements that are all the same)
             */

            // count nodes responded
            recvCounter++;

            BEValue beQueried = response.dictionary.get("id");
            BEValue beToken = response.dictionary.get("token");
            BEValue beNodes = response.dictionary.get("nodes");
            BEValue beValues = response.dictionary.get("values");

            if ((beQueried == null) || !HashId.validate(beQueried.bString)) {
                // id of the answered node must be valid,
                // skip this answer otherwise
                return OPERATION_RERUN;
            }

            long now = System.currentTimeMillis();

            // save active token for the answered node and
            // update state in the routing (case of unknown node)
            HashId queriedNodeId = HashId.wrap(beQueried.bString);
            for (int i = 0; i < nodes.size(); i++) {
                RoutingTable.RemoteNode rNode = nodes.get(i);
                if (rNode.id.equals(queriedNodeId)) {
                    if (DEBUG) {
                        if ((rNode.token != null) && !Arrays.equals(rNode.token, beToken.bString)) {
                            System.out.println(address.toString()
                                            + " get_peers returned another token (need support?)  time:" + (now - rNode.timeToken));
                        }
                    }
                    rNode.token = beToken.bString;

                    // we don't know if original node is from the routing or not,
                    // so try to update there
                    node.routing.update(rNode);
                    break;
                }
            }

            if ((beNodes != null) && (beNodes.bString != null)) {
                // queried node knows more close nodes...
                List<RoutingTable.RemoteNode> remoteNodes = node.parseNodesCompactForm(beNodes.bString);
                foundNodes.addAll(remoteNodes);
            }

            if ((beValues != null) && beValues.isListNotEmpty()) {
                // queried node knows direct peers with the target hash (torrent)
                for(BEValue bePeer: beValues.list) {
                    InetSocketAddress isa = node.parsePeerCompactForm(bePeer.bString);
                    if (isa != null) {
                        foundPeers.add(isa);
                    }
                }
            }

            // end operation in place (could be done in update after timeout)
            if (nodes.size() <= recvCounter) {
                if (callback != null) {
                    callback.accept(foundNodes, foundPeers);
                }
                return OPERATION_FINISH;
            } else {
                return OPERATION_RERUN;
            }
        }
    }


    /**
     * runs announce_peer operation against the list of specific nodes,
     * sends multiple queries in bounds of one operation,
     *
     * nodes MUST be previously queried with get_peers to receive
     * active tokens for announcing, nodes witout token will be skipped
     */
    private static class AnnouncePeerOperation extends Operation {

        /**
         * template prefix of the message
         */
        private final static byte[] TEMPLATE = "d1:t2:TX1:y1:q1:q13:announce_peer1:ad"
                .getBytes(StandardCharsets.UTF_8);
        /**
         * position of transaction id in the template
         */
        private static final int TEMPLATE_POS_TX        = 6;

        /**
         * list of nodes to send announce too
         */
        private final List<RoutingTable.RemoteNode> nodes;
        /**
         * hash of the torrent we are announcing
         */
        private final HashId hash;
        /**
         * optional callback to be notified on finish
         */
        private final Consumer<Boolean> callback;
        /**
         * port of the linked peer used for peer-2-peer communications
         */
        private final int port;

        /**
         * allowed constructor
         * @param _node ref to local node
         * @param _nodes list of nodes to send queries too, could be modified - filtered out
         * @param _hash hash to announce
         * @param _port port of the linked peer for peer-2-peer communications
         * @param _callback callback to send results too
         */
        public AnnouncePeerOperation(
                Node _node,
                List<RoutingTable.RemoteNode> _nodes,
                HashId _hash,
                int _port,
                /* boolean implied */
                Consumer<Boolean> _callback)
        {
            super(_node);
            hash = _hash;
            callback = _callback;
            port = _port;

            nodes = _nodes;

            // remove nodes we don't have token to communicate with,
            // that must be done outside of this operation
            for (int i = nodes.size() - 1; 0 <= i; i--) {
                RoutingTable.RemoteNode rNode = nodes.get(i);
                if ((rNode.token == null) || (rNode.token.length == 0)) {
                    nodes.remove(i);
                }
            }
        }

        /**
         * populates send buffer with operation message
         * review: remove extra copy of the id part
         * @param buffer buffer to populate
         * @param tx transaction id, only 2 low bytes will be used
         * @param token token to use
         */
        void populateSendBuffer(ByteBuffer buffer, byte[] tx, byte[] token) {
            buffer.clear();

            buffer.put(TEMPLATE);
            buffer.put(TEMPLATE_POS_TX, tx[0]).put(TEMPLATE_POS_TX + 1, tx[1]);

            buffer.put("2:id20".getBytes(StandardCharsets.UTF_8)).put(node.id.getBytes());
            buffer.put("9:info_hash20".getBytes(StandardCharsets.UTF_8)).put(hash.getBytes());

            buffer.put("5:token".getBytes(StandardCharsets.UTF_8));
            buffer.put(String.valueOf(token.length).getBytes(StandardCharsets.UTF_8));
            buffer.put((byte)':');
            buffer.put(token);

            buffer.put("4:porti".getBytes(StandardCharsets.UTF_8));
            buffer.put(String.valueOf(port).getBytes(StandardCharsets.UTF_8));
            buffer.put((byte)'e');

            buffer.put("12:implied_porti1e".getBytes(StandardCharsets.UTF_8));

            buffer.put((byte)'e');
            buffer.put((byte)'e');

            buffer.flip();
        }

        @Override
        boolean update()
        {
            if ((recvCounter == nodes.size()) ||
                    (Operation.MAX_SEND_ERRORS < errCounter) ||
                    (timestamp + timeout < System.currentTimeMillis()))
            {
                if (callback != null) {
                    callback.accept(true);
                }
                // indicate operation is over
                return OPERATION_FINISH;
            }

            while (sentCounter < nodes.size())
            {
                RoutingTable.RemoteNode rNode = nodes.get(sentCounter);
                populateSendBuffer(node.sendBuffer, transaction, rNode.token);
                // prepare buffer for re-read
                node.sendBuffer.rewind();

                boolean isSent = send(rNode.address);
                if (!isSent) {
                    // indicate the operation needs a re-run
                    return OPERATION_RERUN;
                }
            }

            // indicate operation is still alive
            return OPERATION_RERUN;
        }


        @Override
        boolean receive(InetSocketAddress address, BEValue response)
        {
            //BEValue beQueried = response.dictionary.get("id");

            recvCounter++;

            // don't wait till check in update if we've received all responses
            if (recvCounter == nodes.size()) {
                if (callback != null) {
                    callback.accept(true);
                }
                return OPERATION_FINISH;
            }

            return OPERATION_RERUN;
        }
    }


    /**
     * Complex operation that iterative find_node operations
     * for the given hash, could be used for bootstrapping with
     * hash of the local node or to find nodes to announce a torrent,
     * calling party could be notified on operation finish via callback
     *
     */
    private static class FindCloseNodesOperation extends Operation
    {
        /**
         * internal states of the operation
         */
        private enum State {
            // initial state
            START,
            // searching nodes close to own id
            NODES
        }

        /**
         * max number of search iterations
         */
        private static final int DEFAULT_MAX_ITERATIONS = 32;
        /**
         * default timeout for the operation to stop, in milliseconds
         */
        static final long DEFAULT_OPERATION_TIMEOUT = 16_000L;

        // max iterations to perform
        private final int iterations;
        // target hash to search
        private final HashId target;
        // callback to be called on completion
        private final Consumer<Boolean> extCallback;
        // optional collection to start bootstrap from
        private final List<InetSocketAddress> seed;


        // tracks iterations while searching for nearest nodes -> nodes -> nodes -> ..
        private int iterationCounter;

        // tracks nodes we have sent request too
        private final Set<RoutingTable.RemoteNode> queried;

        // nodes received and to be queried with the next wave
        private List<RoutingTable.RemoteNode> nextNodes;

        // tracks completion of child operations
        private boolean callbackCalled;

        // current state of the operation
        private State state;


        /**
         * allowed constructor with full customization control,
         * operation will start from _seed collections of specified, otherwise
         * the closest nodes from the routing table will be used
         * @param _node ref to local node
         * @param _target hash to search for
         * @param _extCallback callback to be called on completion with true parameter
         *                     if routing has active nodes in it and false otherwise
         * @param _timeout operations' timeout
         * @param _iterations max allowed iterations
         * @param _seed optional initial collection to start search from (bootstrap mode)
         */
        public FindCloseNodesOperation(
                Node _node, HashId _target, Consumer<Boolean> _extCallback,
                long _timeout, int _iterations, List<InetSocketAddress> _seed)
        {
            super(_node, System.currentTimeMillis(), _timeout);

            target = _target;
            iterations = _iterations;
            extCallback = _extCallback;
            seed = _seed;

            queried = new HashSet<>();
            nextNodes = new ArrayList<>(RoutingTable.Bucket.K);

            state = State.START;
        }

        /**
         * allowed simplified constructor for frequent operations
         * @param _node ref to local node
         * @param _target target hash to seach for
         * @param _extCallback optional callback to notify
         */
        public FindCloseNodesOperation(
                Node _node, HashId _target, Consumer<Boolean> _extCallback)
        {
            this(_node, _target, _extCallback, DEFAULT_OPERATION_TIMEOUT, DEFAULT_MAX_ITERATIONS, null);
        }


        @Override
        boolean update()
        {
            // nodes that are being queried on the current iteration
            List<RoutingTable.RemoteNode> nodes;

            // global timeout protection
            if (timestamp + timeout < System.currentTimeMillis())
            {
                if (extCallback != null) {
                    extCallback.accept(node.routing.hasAliveNodes());
                }
                return OPERATION_FINISH;
            }

            if (state == State.START)
            {
                if ((seed != null) && !seed.isEmpty()) {
                    // use seed collection if specified, we don't have IDs
                    // so just create temporary nodes
                    HashId zero = HashId.zero();
                    nodes = seed.stream()
                            .map(a -> node.routing.new RemoteNode(zero, a))
                            .collect(Collectors.toList());
                } else {
                    // query routing table for closest nodes known,
                    // at least one node is required
                    nodes = node.routing.findClosestNodes(node.id);
                }

                if (nodes.isEmpty()) {
                    // nowhere to search, finish the search
                    if (extCallback != null) {
                        extCallback.accept(node.routing.hasAliveNodes());
                    }
                    return OPERATION_FINISH;
                }

                queried.addAll(nodes);

                // run search for the specified hash
                node.addOperation(new FindNodeOperation(node, nodes, target, rns -> {
                    callbackCalled = true;
                    nextNodes = rns;
                }));

                state = State.NODES;

                return OPERATION_RERUN;
            }

            if (state == State.NODES)
            {
                // wait for the result of the current wave
                if (!callbackCalled) {
                    return OPERATION_RERUN;
                }
                callbackCalled = false;

                // increment number of waves
                iterationCounter++;
                if (DEBUG) {
                    System.out.println("FindCloseNodes: iteration: " + iterationCounter);
                }

                // nodes are independent and could return
                // nodes we have already queried, throw such nodes out
                nextNodes.removeAll(queried);

                // insert all new nodes into routing table
                for (RoutingTable.RemoteNode rNode: nextNodes) {
                    node.routing.insert(rNode);
                }

                // have we reached the end of iterations?
                if (nextNodes.isEmpty() || (iterations < iterationCounter)) {
                    // ok, let's use the last list of nodes
                    if (DEBUG) {
                        System.out.println("FindCloseNodes: finished, nodes in routing: " + node.routing.count());
                    }
                    if (extCallback != null) {
                        extCallback.accept(node.routing.hasAliveNodes());
                    }
                    return OPERATION_FINISH;
                }

                // track all queried (or errors)
                queried.addAll(nextNodes);

                // run next wave
                nodes = nextNodes;
                nextNodes = null;
                node.addOperation(new FindNodeOperation(node, nodes, target, rns -> {
                    callbackCalled = true;
                    nextNodes = rns;
                }));

                return OPERATION_RERUN;
            }

            return OPERATION_RERUN;
        }

    }


    /**
     * Complex operation that runs bootstrapping procedure
     * on local node startup
     * cases:
     * 1. fresh start that needs neighbours search
     * 2. reload that must only publish itself (search new neighbours too??)
     */
    private static class BootstrapOperation extends FindCloseNodesOperation {
        /**
         * max number of search iterations
         */
        private static final int MAX_ITERATIONS = 100;
        /**
         * default timeout for bootstrap operation to stop, in milliseconds
         */
        static final long OPERATION_TIMEOUT = 16_000L;

        /**
         * allowed constructor
         * operation will start from the seed collection,
         * nodes from the seed collection are will not be added to the routing table
         * @param _node ref to local node
         * @param _seed collection to start bootstrapping from
         * @param _extCallback callback to be called on completion with true parameter
         *                     if routing has active nodes in it and false otherwise
         */
        public BootstrapOperation(Node _node, List<InetSocketAddress> _seed, Consumer<Boolean> _extCallback) {
            super(_node, _node.id, _extCallback, OPERATION_TIMEOUT, MAX_ITERATIONS, _seed);
        }
    }


    /**
     * Complex operation to searches for peers that stores
     * any specific torrent (hash)
     */
    private static class FindTorrentPeersOperation extends Operation {
        /**
         * max number of iteration during search process
         */
        private static final int MAX_ITERATIONS = 100;
        /**
         * max number of closest nodes to use on each iteration
         */
        private static final int MAX_NODES_FOR_ITERATION = 16;

        /**
         * target hash to search
         */
        private final HashId target;

        // tracks nodes we have sent request too
        Set<RoutingTable.RemoteNode> queried;

        // all peers found
        Set<InetSocketAddress> peers;

        // nodes that are being queried on the current iteration
        //List<RoutingTable.RemoteNode> nodes;

        // nodes found on the current iteration
        List<RoutingTable.RemoteNode> nodesFound;
        // peers found on the current iteration
        List<InetSocketAddress> peersFound;

        // tracks iterations while searching for nearest nodes nodes -> nodes -> nodes -> ..
        int iterationCounter;

        // completion indicator for child operations
        boolean callbackCalled;

        // callback to be notified on finish
        Consumer<Set<InetSocketAddress>> extCallback;

        // service list of distinct nodes for the next iteration
        List<RoutingTable.RemoteNode> nodesDistinct;
        // distances to distinct nodes
        HashId[] nodesDistinctDistances;

        /**
         * internal states of the operation
         */
        private enum State {
            // initial state
            START,
            // searching nodes close to own id
            NODES
        }

        // current state of the operation
        private State state;

        /**
         * allowed constructor
         * @param _node ref to local node
         * @param _target target hash to search for
         * @param _extCallback callback to call on finish
         */
        public FindTorrentPeersOperation(Node _node, HashId _target, Consumer<Set<InetSocketAddress>> _extCallback) {
            super(_node);
            target = _target;
            extCallback = _extCallback;
            queried = new HashSet<>();
            peers = new HashSet<>();

            nodesDistinct = new ArrayList<>(MAX_NODES_FOR_ITERATION);
            nodesDistinctDistances = new HashId[MAX_NODES_FOR_ITERATION];

            state = State.START;
        }


        @Override
        boolean update()
        {
            // global timeout protection
            if (timestamp + timeout < System.currentTimeMillis())
            {
                if (extCallback != null) {
                    extCallback.accept(peers);
                }
                return OPERATION_FINISH;
            }

            if (state == State.START)
            {
                // query routing table for closest nodes known
                List<RoutingTable.RemoteNode> nodes = node.routing.findClosestNodes(target);
                if (nodes.isEmpty()) {
                    // nowhere to search, finish
                    if (extCallback != null) {
                        extCallback.accept(peers);
                    }
                    return OPERATION_FINISH;
                }

                queried.addAll(nodes);

                // run initial search for target torrent
                node.addOperation(new GetPeersOperation(node, nodes, target, (n, p) -> {
                    callbackCalled = true;
                    nodesFound = n;
                    peersFound = p;
                }));

                state = State.NODES;
            }


            if (state == State.NODES)
            {
                // wait for the result of the current wave
                if (!callbackCalled) {
                    return OPERATION_RERUN;
                }
                callbackCalled = false;

                // increment number of waves
                iterationCounter++;

                // update collection of all found peers
                peers.addAll(peersFound);

                // NOTE
                // foundNodes are 'unlinked' from the routing table,
                // they are new objects not stores in the tree

                // queried nodes are independent and could return
                // nodes we have already queried, throw such nodes out
                nodesFound.removeAll(queried);

                // limit list of nodes as it could grow
                // very quickly: 8 nodes --> 64 --> 512 --> ...
                // remove duplicates and use only closest ones
                nodesDistinct.clear();
                for (RoutingTable.RemoteNode rNode: nodesFound) {
                    node.routing.populateClosestList(target, nodesDistinct, nodesDistinctDistances, rNode);
                }

                // have we reached the end of iterations ?
                if (nodesDistinct.isEmpty() || (MAX_ITERATIONS < iterationCounter)) {
                    if (extCallback != null) {
                        extCallback.accept(peers);
                    }
                    return OPERATION_FINISH;
                }

                // track all queried (or errors)
                queried.addAll(nodesDistinct);

                // run next wave, this will insert nodes into the routing
                node.addOperation(new GetPeersOperation(node, nodesDistinct, target, (n, p) -> {
                    callbackCalled = true;
                    nodesFound = n;
                    peersFound = p;
                }));
                return OPERATION_RERUN;
            }

            return OPERATION_RERUN;
        }
    }


    /**
     * Complex operation to announce torrent being locally downloaded,
     * performs search for nodes closest to the hash of the torrent
     * and then announces to them
     * todo: include local node to peers
     */
    private static class AnnounceTorrentOperation extends Operation
    {
        // default timeout for this operation to stop, in milliseconds,
        // not used really
        static final long OPERATION_TIMEOUT = 16_000L;

        // target hash to announce
        private final HashId hash;

        /**
         * allowed constructor
         * @param _node ref to local node
         * @param _hash target hash to announce
         */
        public AnnounceTorrentOperation(Node _node, HashId _hash) {
            super(_node, System.currentTimeMillis(), OPERATION_TIMEOUT);
            hash = _hash;
        }

        @Override
        boolean update()
        {
            // run search to populate routing with nodes close to hash
            node.addOperation(new FindCloseNodesOperation(node, hash, (b) -> {
                // get closest nodes and announce to them
                List<RoutingTable.RemoteNode> nodes = node.routing.findClosestNodes(hash);
                node.addOperation(new AnnouncePeerOperation(node, nodes, hash, node.peerPort, null));
            }));

            // finish operation,
            // process will be managed from lambda
            return OPERATION_FINISH;
        }

    }


    /**
     * artificial operation to get state of the node (including routing table) from
     * the main thread without any synchronization inside node's code
     */
    private static class GetNodeStateOperations extends Operation
    {
        // callback to be called with state data
        private final Consumer<byte[]> callback;

        /**
         * allowed constructor
         * @param _node ref to local node
         * @param _callback callback to call with state data
         */
        public GetNodeStateOperations(Node _node, Consumer<byte[]> _callback) {
            super(_node);
            callback = _callback;
        }

        @Override
        boolean update() {
            // this must be done in update() as we need
            // synchronization while accessing state
            if (callback != null) {
                byte[] state = node.getState();
                callback.accept(state);
            }
            return OPERATION_FINISH;
        }
    }

    /**
     * base class for response operations, generated on queries received
     * from external nodes
     */
    private static abstract class ResponseOperation extends Operation  {
        /**
         * address of the node that sent us the original query
         */
        protected InetSocketAddress address;
        /**
         * parsed query received
         */
        protected BEValue query;

        // transaction id extracted from the query
        protected byte[] tx;
        // requesting node id extracted
        protected byte[] id;
        // attributes dict extracted
        protected Map<String, BEValue> a;

        // indicates if query is valid or not,
        // used to stop the operation in the base class
        protected boolean queryCorrect = true;

        /**
         * allowed constructor
         * @param _node ref to local node
         * @param _timestamp start time of the operation
         * @param _timeout timeout for the operation
         * @param _address querying node address
         * @param _query query
         */
        protected ResponseOperation(
                Node _node,
                long _timestamp, long _timeout,
                InetSocketAddress _address,
                BEValue _query)
        {
            super(_node, _timestamp, _timeout);
            address = _address;
            query = _query;
        }

        /**
         * allowed constructor,
         * uses default timeout and current time
         * @param _node ref to local node
         * @param _address querying node address
         * @param _query query
         */
        protected ResponseOperation(Node _node,
                                    InetSocketAddress _address,
                                    BEValue _query)
        {
            this(_node, System.currentTimeMillis(), Operation.DEFAULT_OPERATION_TIMEOUT, _address, _query);
        }

        /**
         * checks and extracts common required parameters
         * @param txRequired is transaction id required
         * @param aRequired is attributes dict required
         * @param idRequired is requesting node is required
         * @return true if all required parameters present and false otherwise
         */
        protected boolean extract(boolean txRequired, boolean aRequired, boolean idRequired)
        {
            BEValue tmp;

            // extract transaction
            if (txRequired) {
                tmp = query.dictionary.get("t");
                if (BEValue.isBStringNotNull(tmp)) {
                    tx = tmp.bString;
                } else {
                    // incorrect query
                    return false;
                }
            }

            // extract parameters
            tmp = query.dictionary.get("a");
            if (BEValue.isDictNotNull(tmp)) {
                a = tmp.dictionary;

                // extract node id
                if (idRequired) {
                    tmp = a.get("id");
                    if (BEValue.isBStringWithLength(tmp, HashId.HASH_LENGTH_BYTES)) {
                        id = tmp.bString;
                    } else {
                        // incorrect query
                        return false;
                    }
                }
            }
            else if (aRequired) {
                return false;
            }

            // all requested parameters present
            // and extracted
            return true;
        }

        /**
         * populates send buffer with operation's message,
         * must ne implemented in child classes with specific data
         * @param buffer buffer to populate
         */
        abstract void populateSendBuffer(ByteBuffer buffer);

        /**
         * adds binary encoded transaction id into the the buffer at current position,
         * this is used for custom transaction ids with size not equals to 2 bytes
         * @param buffer destination buffer
         * @param tx transaction id
         */
        protected void populateTransaction(ByteBuffer buffer, byte[] tx) {
            buffer.put(String.valueOf(tx.length).getBytes(StandardCharsets.UTF_8));
            buffer.put((byte)':');
            buffer.put(tx);
        }

        @Override
        boolean update()
        {
            if ((!queryCorrect)
                    ||(Operation.MAX_SEND_ERRORS < errCounter)
                    || (timestamp + timeout < System.currentTimeMillis()))
            {
                // can't send for some time
                // indicate operation is over
                return OPERATION_FINISH;
            }

            // let specific child class to populate it
            populateSendBuffer(node.sendBuffer);

            // prepare buffer for re-read,
            // this is more a protection if it wan't done in #populateSendBuffer
            node.sendBuffer.rewind();

            boolean isSent = send(address);
            if (!isSent) {
                // try to resend on the next update() turn
                return OPERATION_RERUN;
            }

            return OPERATION_FINISH;
        }
    }

    /**
     * Operation for sending response to a received ping query message
     */
    private static class PingResponseOperation extends ResponseOperation {

        /**
         * template for outgoing message,
         * [?] bytes transaction id and 20 bytes hash id must be substituted
         * note: usually transaction id is 2 bytes, but external side could send any size,
         * so why TX is at end of the template to allow extension
         */
        static byte[] TEMPLATE = "d1:y1:r1:rd2:id20:XXXXXXXXXXXXXXXXXXXXe1:t2:TXe"
                .getBytes(StandardCharsets.UTF_8);

        /**
         * position of hash id in the template
         */
        static final int TEMPLATE_POS_HASH = 18;

        /**
         * position of transaction element (len:data)
         */
        static final int TEMPLATE_POS_TX_ELEMENT = 42;

        /**
         * allowed constructor
         * @param _node ref to local node
         * @param _address remote address
         * @param _query ping query
         */
        public PingResponseOperation(Node _node, InetSocketAddress _address, BEValue _query)
        {
            super(_node, _address, _query);
            // all parameters must present
            queryCorrect = extract(true, true, true);
        }

        /**
         * populates send buffer with operation message
         * review: remove extra copy of the id part
         * @param buffer buffer to populate
         */
        void populateSendBuffer(ByteBuffer buffer)
        {
            buffer.clear();
            buffer.put(TEMPLATE);
            buffer.put(TEMPLATE_POS_HASH, node.id.getBytes());

            if (tx.length == 2) {
                // most common case:
                // transaction id has standard length, insert into template
                buffer.put(TEMPLATE_POS_TX_ELEMENT + 2, tx);
            } else {
                // custom transaction id length,
                // generate full element
                buffer.position(TEMPLATE_POS_TX_ELEMENT);
                populateTransaction(buffer, tx);
                // end of enclosing dict
                buffer.put((byte)'e');
            }

            buffer.flip();
        }
    }


    /**
     * Operation for sending response to a received find_node query message
     */
    private static class FinNodeResponseOperation extends ResponseOperation {

        /*
         * find_node Query = {"t":"aa", "y":"q", "q":"find_node", "a": {"id":"abcdefghij0123456789", "target":"mnopqrstuvwxyz123456"}}
         * bencoded = d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe
         *
         * Response = {"t":"aa", "y":"r", "r": {"id":"0123456789abcdefghij", "nodes": "def456..."}}
         * bencoded = d1:rd2:id20:0123456789abcdefghij5:nodes9:def456...e1:t2:aa1:y1:re
         *
         */

        /**
         * list of nodes from the routing table, closes to the targer hash
         */
        private List<RoutingTable.RemoteNode> nodes;

        /**
         * allowed constructor
         * @param _node ref to local node
         * @param _address remote address
         * @param _query ping query
         */
        public FinNodeResponseOperation(Node _node, InetSocketAddress _address, BEValue _query)
        {
            super(_node, _address, _query);
            // all parameters must present
            queryCorrect = extract(true, true, true);
            // extract specific parameters
            if (queryCorrect) {
                BEValue tmp = a.get("target");
                if (BEValue.isBStringWithLength(tmp, HashId.HASH_LENGTH_BYTES)) {
                    byte[] target = tmp.bString;
                    nodes = node.routing.findClosestNodes(HashId.wrap(target));
                    // just the protection as compact form designed only for ipv4
                    for (int i = nodes.size() - 1; 0 <= i; i--) {
                        if (!(nodes.get(i).address.getAddress() instanceof Inet4Address)) {
                            nodes.remove(i);
                        }
                    }
                } else {
                    queryCorrect = false;
                }
            }

        }

        /**
         * populates send buffer with operation message
         * review: remove extra copy of the id part
         * @param buffer buffer to populate
         */
        void populateSendBuffer(ByteBuffer buffer)
        {
            buffer.clear();

            buffer.put("d1:y1:r1:t2:".getBytes(StandardCharsets.UTF_8));
            if (tx.length == 2) {
                // most common case:
                // transaction id has standard length, insert into template
                buffer.put(tx);
            } else {
                // custom transaction id length,
                // rollback length prefix "2:"
                buffer.position(buffer.position() - 2);
                populateTransaction(buffer, tx);
            }

            // node id
            buffer.put("1:rd2:id20:".getBytes(StandardCharsets.UTF_8));
            buffer.put(node.id.getBytes());

            // nodes
            buffer.put("5:nodes".getBytes(StandardCharsets.UTF_8));
            buffer.put(String.valueOf(nodes.size() * 26).getBytes(StandardCharsets.UTF_8));
            buffer.put((byte)':');

            for (RoutingTable.RemoteNode rn: nodes) {
                buffer.put(rn.id.getBytes());
                buffer.put(rn.address.getAddress().getAddress());
                int port = rn.address.getPort();
                buffer.put((byte)((port >> 8) & 0xFF));
                buffer.put((byte)(port & 0xFF));
            }

            // dictionaries end
            buffer.put((byte)'e');
            buffer.put((byte)'e');

            buffer.flip();
        }

    }


    /**
     * Operation for sending response to a received get_peers query message,
     * we always use the same token for the same external node (ip address)
     * and don't depend on id of the torrent requested
     */
    private static class GetPeersResponseOperation extends ResponseOperation {

        /*
         * get_peers Query = {"t":"aa", "y":"q", "q":"get_peers", "a": {"id":"abcdefghij0123456789", "info_hash":"mnopqrstuvwxyz123456"}}
         *
         * Response with peers = {"t":"aa", "y":"r", "r": {"id":"abcdefghij0123456789", "token":"aoeusnth", "values": ["axje.u", "idhtnm"]}}
         * bencoded = d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re
         *
         * Response with closest nodes = {"t":"aa", "y":"r", "r": {"id":"abcdefghij0123456789", "token":"aoeusnth", "nodes": "def456..."}}
         * bencoded = d1:rd2:id20:abcdefghij01234567895:nodes9:def456...5:token8:aoeusnthe1:t2:aa1:y1:re
         *
         */

        /**
         * target info hash to look for
         */
        private byte[] target;
        /**
         * list of nodes from the routing table, closes to the target hash
         */
        private List<RoutingTable.RemoteNode> nodes;

        private Set<RoutingTable.RemoteNode> peers;

        /**
         * instance of remote node we are communicating with
         */
        private RoutingTable.RemoteNode rNode;

        /**
         * allowed constructor
         * @param _node ref to local node
         * @param _address remote address
         * @param _query ping query
         */
        public GetPeersResponseOperation(Node _node, InetSocketAddress _address, BEValue _query)
        {
            super(_node, _address, _query);
            // all parameters must present
            queryCorrect = extract(true, true, true);

            // extract specific parameters
            if (queryCorrect) {
                BEValue tmp = a.get("info_hash");
                if (BEValue.isBStringWithLength(tmp, HashId.HASH_LENGTH_BYTES)) {
                    target = tmp.bString;
                } else {
                    queryCorrect = false;
                }
            }

            if (!queryCorrect) {
                return;
            }

            // query is correct so
            // collect all data we'll need for sending the reply
            peers = node.peers.get(HashId.wrap(target));
            if ((peers == null) || peers.isEmpty())
            {
                // we don't know nodes announced themselves for this torrent,
                // let's return closest nodes
                nodes = node.routing.findClosestNodes(HashId.wrap(target));
                // just the protection as compact form designed only for ipv4
                for (int i = nodes.size() - 1; 0 <= i; i--) {
                    if (!(nodes.get(i).address.getAddress() instanceof Inet4Address)) {
                        nodes.remove(i);
                    }
                }
            } else {
                // just the protection as compact form designed only for ipv4
                peers.removeIf(rNode -> !(rNode.address.getAddress() instanceof Inet4Address));
            }

            HashId hid = HashId.wrap(id);

            // check if we've already communicated with this node
            // and have it in 'active' set
            rNode = node.tokens.get(hid);

            if (rNode == null) {
                // seems this is a fresh request, check
                // if have this node in routing
                rNode = node.routing.getRemoteNode(hid);
            }

            if (rNode == null) {
                // seems we know nothing about the node or
                // it was removed from all places (capacity limits?),
                // allocate new instance
                rNode = node.routing.new RemoteNode(hid, address);

                // this could fail due to capacity limitations,
                // but that's not a problem
                node.routing.insert(rNode);

                // finally store it as active
                node.tokens.put(hid, rNode);
            }

            rNode.updateLastActivityTime();
        }

        /**
         * populates send buffer with operation message
         * review: remove extra copy of the id part
         * @param buffer buffer to populate
         */
        void populateSendBuffer(ByteBuffer buffer)
        {
            buffer.clear();

            buffer.put("d1:y1:r1:t2:".getBytes(StandardCharsets.UTF_8));
            if (tx.length == 2) {
                // most common case:
                // transaction id has standard length, insert into template
                buffer.put(tx);
            } else {
                // custom transaction id length,
                // rollback length prefix "2:"
                buffer.position(buffer.position() - 2);
                populateTransaction(buffer, tx);
            }

            // node id
            assert node.id.getBytes().length == 20: "node id must be 20 bytes";
            buffer.put("1:rd2:id20:".getBytes(StandardCharsets.UTF_8));
            buffer.put(node.id.getBytes());

            // token
            byte[] tokenLocal = rNode.getTokenLocal();
            assert tokenLocal.length == 8: "generated token must be 8 bytes";
            buffer.put("5:token8:".getBytes(StandardCharsets.UTF_8));
            buffer.put(tokenLocal);

            if ((peers == null) || peers.isEmpty())
            {
                // peers are missing, generate closest nodes,
                // note: this is used even for empty list
                buffer.put("5:nodes".getBytes(StandardCharsets.UTF_8));
                buffer.put(String.valueOf(nodes.size() * 26).getBytes(StandardCharsets.UTF_8));
                buffer.put((byte)':');

                for (RoutingTable.RemoteNode rn: nodes) {
                    buffer.put(rn.id.getBytes());
                    buffer.put(rn.address.getAddress().getAddress());
                    int port = rn.address.getPort();
                    buffer.put((byte)((port >> 8) & 0xFF));
                    buffer.put((byte)(port & 0xFF));
                }
            }
            else {
                // generate known peers
                buffer.put("6:valuesl".getBytes(StandardCharsets.UTF_8));
                for (RoutingTable.RemoteNode rn: peers) {
                    buffer.put((byte)'6');
                    buffer.put((byte)':');

                    buffer.put(rn.address.getAddress().getAddress());
                    int port = rn.address.getPort();
                    buffer.put((byte)((port >> 8) & 0xFF));
                    buffer.put((byte)(port & 0xFF));
                }
                buffer.put((byte)'e');
            }

            // dictionaries end
            buffer.put((byte)'e');
            buffer.put((byte)'e');

            buffer.flip();
        }
    }


    /**
     * Operation for sending response to a received announce_peer query message
     */
    private static class AnnounceResponseOperation extends ResponseOperation {

        /**
         * template for outgoing message,
         * [?] bytes transaction id and 20 bytes hash id must be substituted
         * note: usually transaction id is 2 bytes, but external side could send any size,
         * so why TX is at end of the template to allow extension
         */
        static byte[] TEMPLATE = "d1:y1:r1:rd2:id20:XXXXXXXXXXXXXXXXXXXXe1:t2:TXe"
                .getBytes(StandardCharsets.UTF_8);
        /**
         * position of hash id in the template
         */
        static final int TEMPLATE_POS_HASH = 18;
        /**
         * position of transaction element (len:data)
         */
        static final int TEMPLATE_POS_TX_ELEMENT = 42;

        /**
         * allowed constructor
         * @param _node ref to local node
         * @param _address remote address
         * @param _query ping query
         */
        public AnnounceResponseOperation(Node _node, InetSocketAddress _address, BEValue _query)
        {
            super(_node, _address, _query);

            byte[] target = null;
            byte[] token = null;
            int port = 0;
            boolean implied = false;

            // all parameters must present
            queryCorrect = extract(true, true, true);

            // extract specific parameters
            if (queryCorrect) {
                BEValue tmp = a.get("info_hash");
                if (BEValue.isBStringWithLength(tmp, HashId.HASH_LENGTH_BYTES)) {
                    target = tmp.bString;
                } else {
                    queryCorrect = false;
                    return;
                }

                tmp = a.get("token");
                if (BEValue.isBStringNotEmpty(tmp)) {
                    token = tmp.bString;
                } else {
                    queryCorrect = false;
                    return;
                }

                tmp = a.get("port");
                if (BEValue.isInteger(tmp)) {
                    port = (int) tmp.integer;
                } else {
                    queryCorrect = false;
                    return;
                }

                tmp = a.get("implied_port");
                if (BEValue.isInteger(tmp) && (tmp.integer != 0)) {
                    implied = true;
                }
            }

            if (!queryCorrect) {
                return;
            }


            // instance of remote node we are communicating with
            RoutingTable.RemoteNode rNode = node.tokens.get(HashId.wrap(id));
            if ((rNode == null)
                    || !Arrays.equals(rNode.getTokenLocal(), token))
            {
                if (DEBUG) {
                    // it's possible for external node to remember different tokens for different torrents
                    // must be checked
                    if ((rNode != null) && !Arrays.equals(rNode.getTokenLocal(), token)) {
                        System.out.println(
                                address.toString() + "  external announce: invalid token (torrent dependent?)");
                    }
                }

                queryCorrect = false;
                return;
            }

            rNode.updateLastActivityTime();

            HashId hashTarget = HashId.wrap(target);

            Set<RoutingTable.RemoteNode> downloaders =
                    node.peers.computeIfAbsent(hashTarget, k -> new HashSet<>());
            downloaders.add(rNode);

            InetSocketAddress peerAddress = address;
            if (!implied) {
                rNode.setPeerPort(port);
                peerAddress = new InetSocketAddress(peerAddress.getAddress(), port);
            } else {
                rNode.setPeerPort(0);
            }

            if (node.callbackAnnounce != null) {
                node.callbackAnnounce.accept(hashTarget, peerAddress);
            }

        }

        /**
         * populates send buffer with operation message
         * review: remove extra copy of the id part
         * @param buffer buffer to populate
         */
        void populateSendBuffer(ByteBuffer buffer)
        {
            buffer.clear();
            buffer.put(TEMPLATE);
            buffer.put(TEMPLATE_POS_HASH, node.id.getBytes());

            if (tx.length == 2) {
                // most common case:
                // transaction id has standard length, insert into template
                buffer.put(TEMPLATE_POS_TX_ELEMENT + 2, tx);
            } else {
                // custom transaction id length,
                // generate full element
                buffer.position(TEMPLATE_POS_TX_ELEMENT);
                populateTransaction(buffer, tx);
                // end of enclosing dict
                buffer.put((byte)'e');
            }

            buffer.flip();
        }
    }

}
