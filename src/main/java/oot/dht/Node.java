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

/**
 * contains local DHT node information together with
 * necessary service parameters, runs service thread
 * to perform network communications.
 * todo: subdivide into node api class and node/thread
 */
public class Node {
    /**
     * switch to allow debug messages to stdout
     */
    static final boolean DEBUG = false;

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
     * background thread for running node communications
     * in bounds of DHT protocol,
     * this is the MAIN thread that handles node and routing table,
     * also this thread will notify callbacks of a calling party
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
    RoutingTable routing = new RoutingTable(this);

    /**
     * stores peers that announced themselves as downloaders
     * of some torrent(s)
     * Map<torrent hash, Set<node>>
     *
     * todo include local node
     */
    Map<HashId, Set<RoutingTable.RemoteNode>> peers = new HashMap<>();

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
    Map<HashId, RoutingTable.RemoteNode> tokens = new HashMap<>();

    /**
     * if registered this callback will be called for any announce_peer
     * message received by this node, could be used to notify torrent client
     * with new peers
     */
    volatile BiConsumer<HashId, InetSocketAddress> callbackAnnounce;

    /**
     * if registered this callback will be called when node
     * becomes bootstrapped, usually called only once,
     * bout could be called more times in case of start-stop-start-stop-.. sequence
     */
    volatile Consumer<Void> callbackBootstrapped;

    // indicates if this node has connection
    // to dht network or not, used to delay operations' requests,
    // doesn't need to be volatile
    boolean bootstrapped = false;

    // transaction id for a new operation initiated by the local node
    // todo: extend to long (as received from the wild)
    private short transaction;

    // port of the peer that accepts peer-2-peer connections,
    // could be zero if the same as DHT must be used
    volatile int peerPort;

    // binary encoding parser used across the node
    private final BEParser parser = new BEParser();

    /**
     * channel to send/receive dht messages,
     * could be replaced with channel/selector
     */
    private DatagramChannel channel;

    /**
     * exchange point for externally added operations,
     * are moved to main collection internally with synchronization
     */
    private final List<Operation> extOperations = new ArrayList<>();

    /**
     * active operations performed by the node,
     * contains all types of dht queries
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
     * todo: review direct access from Operations
     */
    final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(1500);

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
     * registers announce callback to be notified when external announce message is received,
     * callback will be called from the node executing thread
     * @param callbackAnnounce callback
     */
    public void setCallbackAnnounce(BiConsumer<HashId, InetSocketAddress> callbackAnnounce) {
        this.callbackAnnounce = callbackAnnounce;
    }

    /**
     * registers bootstrap callback to be notified when node becomes alive
     * callback will be called from the node executing thread
     * @param callbackBootstrapped callback
     */
    public void setCallbackBootstrapped(Consumer<Void> callbackBootstrapped) {
        this.callbackBootstrapped = callbackBootstrapped;
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
     * @param callback callback to be notified
     */
    public void findPeers(HashId hash, Consumer<Set<InetSocketAddress>> callback) {
        synchronized (extOperations) {
            extOperations.add(new FindTorrentPeersOperation(this, hash, callback));
        }
    }

    /**
     * runs state collecting operation on the main node's thread,
     * callback will be notified in bounds of node's thread,
     * could be notified with null data if state is missing (not bootstrapped)
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
    public synchronized void start() throws IOException
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

        // initiate bootstrap process in any case,
        // this will update remote nodes if exist
        addOperation(new BootstrapOperation(this, null, null));
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

        // will be used on possible subsequent starts
        bootstrapped = false;
    }

    /**
     * tries to generate unique id for this node,
     * @return generated id
     */
    private HashId generateUniqueId() {
        try {
            // this method is usually called only once, so just create random here
            SecureRandom random = SecureRandom.getInstanceStrong();
            byte[] data = new byte[HashId.HASH_LENGTH_BYTES];
            random.nextBytes(data);
            return HashId.wrap(data);
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
     * @throws IOException af any
     */
    private void reinit() throws IOException
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
    void addOperation(Operation operation) {
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

        // todo run periodical updates to find more nodes
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
                // leave till the next turn (todo: continue?)
                return;
            }

            if (DEBUG) {
                ProtocolDumper.dumpMessage(false, false, address, parsed);
            }

            // is this a response message?
            BEValue y = parsed.dictionary.get("y");

            if (y == null) {
                // skip incorrect messages without type
                continue;
            }

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
    boolean send(InetSocketAddress address) {
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
    byte[] getState()
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
                os.write((rn.address.getPort() >> 8) & 0xFF);
                os.write( rn.address.getPort() & 0xFF);
            }
            else if (address instanceof Inet6Address) {
                // address type and address
                os.write(2);
                os.writeBytes(address.getAddress());
                os.write((rn.address.getPort() >> 8) & 0xFF);
                os.write( rn.address.getPort() & 0xFF);
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
                if (DEBUG) System.out.println("RoutingTable: incorrect state (some errors), can't restore");
            }
            return success;
        } catch (Exception ignored) {
            if (DEBUG) System.out.println("RoutingTable: incorrect state (too short), can't restore");
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
        // must apply id right here as child
        // elements will use it on insert
        id = HashId.wrap(tmp);

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
                port = buffer.getShort() & 0xFFFF;
            } else if (type == 2) {
                tmp = new byte[16];
                buffer.get(tmp);
                iAddr = Inet6Address.getByAddress(tmp);
                port = buffer.getShort() & 0xFFFF;
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
        routing = rTable;

        return true;
    }
}
