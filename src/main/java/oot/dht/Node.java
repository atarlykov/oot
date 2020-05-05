package oot.dht;

import oot.PeerConnection;
import oot.Torrent;
import oot.be.BEParser;
import oot.be.BEValue;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.net.*;

import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * contains local DHT node information together with
 * necessary service parameters
 */
public class Node {
    /**
     * static node id
     */
    private static HashId LOCAL_NODE_ID = new HashId(new byte[] {62, 96, 84, -63, -89, 88, -127, 22, 91, 115, 101, -64, 27, -89, -7, 118, 36, -94, -66, -102});

    /**
     * max number of packets received in one turn
     */
    private static int RECIVE_PACKETS_MAX = 16;

    // persistent node id (could be the same as client id)
    HashId id = LOCAL_NODE_ID;

    // review: must be persisted and loaded back
    public RoutingTable routing = new RoutingTable(this);

    // indicates if this node has connection
    // to dht network or not, used to delay operations' requests
    private boolean isBootstrapped = false;

    // need handshake first to determine if dht is supported
    // review: must be outside of this class
    //public void bootstrapFromPeers(List<InetSocketAddress> peers) {    }


    // transaction id for a new operation initiated by the local node
    short transaction;

    BEParser parser = new BEParser();

    /**
     * channel to send/receive dht messages
     */
    DatagramChannel channel;
    /**
     * selector to be notified about received messages
     */
    //Selector selector;



    // that are dht nodes (torrent file nodes),
    // this will NEED ping to find their node ids
    public void bootstrapFromNodes(List<InetSocketAddress> nodes) {    }

    // get peers for torrent
    public List<PeerConnection> findPeers(byte[] hash) {return null;}

    // starts
    public void triggerUpdatePeers(Torrent t) {}


    private static final Cleaner cleaner = Cleaner.create();
    private final Cleaner.Cleanable cleanable = cleaner.register(this, () -> {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (Exception e) {
        }
    });


    /**
     * @return id of the node
     */
    public HashId getId() {
        return id;
    }

    /**
     * review: now initialized from scratch, not binding to a specfic port
     * @throws Exception if any
     */
    public void init() throws Exception {
        channel = DatagramChannel.open(StandardProtocolFamily.INET);
        channel.bind(null);
        //InetSocketAddress localAddress = (InetSocketAddress) channel.getLocalAddress();
        //channel.setOption(StandardSocketOptions.SO_RCVBUF, );
        //channel.setOption(StandardSocketOptions.SO_SNDBUF, );
        channel.configureBlocking(false);
    }


    /**
     * Base class for all operations that run inside the local node,
     * includes simple protocol operation, complex operations on top of protocol messages,
     * possible utility/maintenance operations and operations, spawned on received messages
     *
     * Provides non blocking way to perform various tasks inside a node.
     */
    private abstract static class Operation {

        // alias for internal methods to indicate operation must be finished
        static final boolean OPERATION_FINISH = true;
        // alias for internal methods to indicate operation must be proceeded with execution
        static final boolean OPERATION_RERUN = false;


        static final int MAX_SEND_ERRORS = 100;
        // TODO: REVERT TO NORMAL
        static final long DEF_TIMEOUT = 1_000L;

        enum Type {
            PING, FIND_NODE, GET_PEERS, ANNOUNCE_PEER
        }
        // type of the operation
        Type type;

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


        // controls numbers of datagrams sent,
        // need this in case if we can't send all datagrams in
        // one 'update' turn
        int sentCounter = 0;
        // controls number of received datagrams for this operation
        int recvCounter = 0;
        // track number of possible error to force end the operation
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
            this(_node, System.currentTimeMillis(), DEF_TIMEOUT);
        }

        /**
         * processes any response from the outer space
         * with transaction id of this operation
         * @param address address of the sending party
         * @param response response received, only the internal response element
         * @return must return true if operation must be discarded after processing
         */
        boolean receive(InetSocketAddress address, BEValue response) {
            return false;
        }

        /**
         * called to notify operation about error received in bounds of it's transaction
         * @param address address of the sending party
         * @param response response received, only the error element
         * @return must return true if operation must be discarded after processing
         */
        boolean error(InetSocketAddress address, BEValue response) {
            return false;
        }

        /**
         * update operation's state, called periodically
         * to track timeouts and proceed with steps inside the operation
         * @return must return true if operation must be discarded after processing
         */
        boolean update() {
            return false;
        }

        /**
         * service method that sends prepared send buffer from the node
         * and tracks local send/error counters
         * @param address destinatino address to send too
         * @return true if datagram was successfully sent and
         * false otherwise (that could be buffer overflow or some io error)
         * TODO: move make call to Node.send
         */
        boolean send(InetSocketAddress address) {
            try {

                // DEBUG
                BEValue message = null;
                try {
                    message = node.parser.parse(node.sendBuffer);
                    node.sendBuffer.rewind();
                } catch (Exception e) {
                }

                int sent = node.channel.send(node.sendBuffer, address);
                if (sent == 0) {

                    // DEBUG
                    ProtocolDumper.dumpMessage(true, true, address, message);

                    // there were no space in system send buffer,
                    // exit and let retry on the next update turn
                    return false;
                }

                // DEBUG
                ProtocolDumper.dumpMessage(true, false, address, message);


                // ok, track successful messages
                sentCounter++;
                return true;
            } catch (IOException e) {
                // something happened, track number of errors
                errCounter++;
                return false;
            }
        }

        /**
         * review: move to utility / refactor
         * @param binary
         * @return
         */
        List<RoutingTable.RemoteNode> parseNodesCompactForm(byte[] binary) {
            int count = binary.length / 26;
            List<RoutingTable.RemoteNode> nodes = new ArrayList<>(count);

            for (int i = 0; i < count; i++) {
                // each compact form is 26 bytes
                int index = i * 26;


                HashId id = new HashId(binary, index);

                index += HashId.HASH_LENGTH_BYTES;
                byte[] addr = Arrays.copyOfRange(binary,
                        index,
                        index + 4);

                index += 4;
                int port = ((Byte.toUnsignedInt(binary[index]) << 8) +
                        Byte.toUnsignedInt(binary[index + 1]));

                InetSocketAddress address;
                try {
                    address = new InetSocketAddress(Inet4Address.getByAddress(addr), port);
                } catch (UnknownHostException e) {
                    // that shouldn't be
                    throw new RuntimeException(e);
                }

                RoutingTable.RemoteNode tmp = node.routing.new RemoteNode(id, address);
                nodes.add(tmp);
            }
            return nodes;
        }

        /**
         * parses binary ip4 address format, 4 bytes address and 2 bytes port as big-endian
         * @param binary array with data, must be of length 6
         * @return parsed address or null if something missing or wrong
         */
        InetSocketAddress parsePeerCompactForm(byte[] binary) {
            if ((binary == null) || (binary.length != 6)) {
                return null;
            }
            byte[] addr = Arrays.copyOfRange(binary, 0, 4);
            int port = ((Byte.toUnsignedInt(binary[4]) << 8) +
                    Byte.toUnsignedInt(binary[5]));

            try {
                return new InetSocketAddress(Inet4Address.getByAddress(addr), port);
            } catch (UnknownHostException e) {
                // that shouldn't be
                return null;
            }
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
         * sets id ot his transaction
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
        boolean testTransaction(byte[] id) {
            return Arrays.equals(transaction, id);
        }
    }

    /**
     * active operations performed by the node,
     * include all types of dht queries
     */
    private List<Operation> operations = new ArrayList<>();

    private ByteBuffer receiveBuffer = ByteBuffer.allocateDirect(1500);
    private ByteBuffer sendBuffer = ByteBuffer.allocateDirect(1500);

    /**
     * generates next transactions id
     * @return next transaction id
     */
    public short nextTransactionId() {
        return transaction++;
    }

    /**
     * adds operation to the list of active operations for execution
     * @param operation operation
     */
    public void addOperation(Operation operation) {
        operation.setTransaction(nextTransactionId());
        operations.add(operation);
    }

    /**
     * incrementally updates node state and processes all queries
     */
    public void update() {
        for (int i = operations.size() - 1; 0 <= i; i--) {
            Operation operation = operations.get(i);
            boolean done = operation.update();
            if (done) {
                operations.remove(i);
            }
        }
    }


    /**
     * processes all received packets from the node channel,
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
     *
     * only "t" gives type of response
     * @throws Exception
     */
    public void receive() {
        // alias
        ByteBuffer buffer = receiveBuffer;

        // counter of received packets
        // is used to check for max allowed packets received in one turn
        int recvCounter = 0;

        do {
            // protect from a massive incoming stream
            if (RECIVE_PACKETS_MAX < recvCounter) {
                return;
            }

            // channel is in non-blocking mode, so this will not block
            SocketAddress address = null;
            try {
                buffer.clear(); // clear buffer to be available for writing
                if ((address = channel.receive(buffer)) == null) {
                    return;     // nothing in the channel
                }
            } catch (Exception e) {
                System.out.println("receive error: " + e.getMessage());
                // todo: tryRecover()
                return;
            }

            recvCounter++;

            // prepare buffer to read the received data
            buffer.flip();

            BEValue parsed = null;
            try {
                parsed = parser.parse(buffer);
            } catch (Exception e) {
                System.out.println("incorrect message format: " + e);
                return;
            }

            // DEBUG
            ProtocolDumper.dumpMessage(false, false, address, parsed);

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
            System.out.println("incorrect reply type");

        } while (true);
    }

    private void processExternalQuery(SocketAddress address, BEValue query) {
        BEValue q = query.dictionary.get("q");
        System.out.println("query message received, skipping: " + new String(q.bString, StandardCharsets.UTF_8));
    }

    private void processExternalError(SocketAddress address, BEValue parsed) {
        BEValue tx = parsed.dictionary.get("t");
        BEValue error = parsed.dictionary.get("e");

        // find by tx and inform about error
        for (int i = 0; i < operations.size(); i++) {
            Operation operation = operations.get(i);
            if ( !operation.testTransaction(tx.bString) ) {
                continue;
            }

            // TODO: operation must validate response to protect from unknown addresses
            if (operation.error((InetSocketAddress) address, error)) {
                // operation indicates it's over,
                // remove it
                operations.remove(i);
            }
            break;
        }
    }

    private void processExternalResponse(SocketAddress address, BEValue parsed) {
        BEValue version = parsed.dictionary.get("v");
        BEValue transaction_id = parsed.dictionary.get("t");
        BEValue response = parsed.dictionary.get("r");

        boolean txFound = false;
        // find by tx and send to process
        for (int i = 0; i < operations.size(); i++) {
            Operation operation = operations.get(i);
            if ( !operation.testTransaction(transaction_id.bString) ) {
                continue;
            }
            txFound = true;
            // TODO: operation must validate response to protect from unknown addresses
            if (operation.receive((InetSocketAddress) address, response)) {
                // operation indicates it's over,
                // remove it
                operations.remove(i);
            }
            break;
        }
        if (!txFound) {
            // log possible error
            System.out.println("REPLY TX WITHOUT OPERATION");
        }
    }

    /**
     * ping operation checks remote peers and/or finds their ids,
     * operation always inserts nodes into routing table with new status,
     * removes nodes if they don't respond or adds new nodes if they
     * were missing in routing table (bootstrapping).
     *
     * uses the same transaction id for ping requests to different addresses
     * in bounds of one operation
     */
    private static abstract class PingOperation extends Operation {

        static byte[] TMPL  = "d1:y1:q1:q4:ping1:t2:TX1:ad2:id20:XXXXXXXXXXXXXXXXXXXXee".getBytes(StandardCharsets.UTF_8);
        static final int TMPL_POS_TX = 21;
        static final int TMPL_POS_ID = 34;


        /**
         * allowed constructor
         * @param _node ref to local node
         */
        public PingOperation(Node _node) {
            super(_node);
        }

        /**
         * review: remove extra copy of the id part
         * @param buffer buffer to populate
         * @param tx transaction id, must be 2 bytes array
         */
        void populateSendBuffer(ByteBuffer buffer, byte[] tx) {
            buffer.clear();
            buffer.put(TMPL);
            buffer.put(TMPL_POS_TX, tx[0]).put(TMPL_POS_TX + 1, tx[1]);
            buffer.put(TMPL_POS_ID, node.id.getBytes());
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
        List<InetSocketAddress> peers;

        // do we need to insert answered nodes into routing or update them if already exist
        boolean updateRouting;

        // consumer to notify ar the end of the operation
        Consumer<List<RoutingTable.RemoteNode>> callback;

        // nodes to send to callback
        List<RoutingTable.RemoteNode> cbNodes;

        /**
         * case of several peers
         * @param _node ref to local node
         * @param _peers peers to ping
         * @param _updateRouting will try to insert answered nodes into routing or update them if already exist
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
         * case of a single peer
         * @param _node ref to local node
         * @param _peer peer to ping
         * @param _updateRouting will try to insert answered nodes into routing or update them if already exist
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
        boolean update() {

            if (Operation.MAX_SEND_ERRORS < errCounter) {
                // finish ping operation due to errors
                if (callback != null) {
                    callback.accept(cbNodes);
                }
                return OPERATION_FINISH;
            }

            if ((recvCounter == peers.size()) ||
                    (timestamp + timeout < System.currentTimeMillis()))
            {
                // all responses received or operation timeout reached,
                // "all" check duplicates one in receive
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
                    // indicate the operation needs a re-run
                    return OPERATION_RERUN;
                }
            }

            // always indicate operation is still alive,
            // it could be finished on timeout or on 'all received'
            return OPERATION_RERUN;
        }

        @Override
        boolean receive(InetSocketAddress address, BEValue response) {

            // count answered peers
            recvCounter++;

            // response format
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
     * ping operation checks node from the routing table,
     * operation always updates nodes in routing table with new status,
     * removes nodes if they don't respond or adds new nodes if they
     * were missing in routing table.
     *
     * uses the same transaction id for ping requests to different addresses
     * in bounds of one operation
     */
    public static class PingNodesInRoutinTableOperation extends PingOperation {

        RoutingTable.Bucket current;
        boolean callbackCalled;

        public PingNodesInRoutinTableOperation(Node _node) {
            super(_node);
            current = node.routing.root;
            // this starts the process
            callbackCalled = true;
        }

        @Override
        boolean update() {

            // check if there is no next level in routing
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

            System.out.println("PNOP: next level");

            // get nodes that are ready to be refreshed
            List<InetSocketAddress> addresses = current.elements.stream()
                    .filter(RoutingTable.RemoteNode::isReadyForPing)
                    .map(rn -> rn.address)
                    .collect(Collectors.toList());

            System.out.println("     ready for ping: " + addresses.size());

            if (addresses.isEmpty()) {
                current = current.child;
                return OPERATION_RERUN;
            }

            // reset processing flag and call operation
            callbackCalled = false;
            node.addOperation(new PingPeersOperation(node, addresses, true, x -> {
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
     * by some another local operation
     *
     * operation returns not more than {@link RoutingTable.Bucket#K} closest elements
     * in the result list (callback)
     *
     * arguments:  {"id" : "<querying nodes id>", "target" : "<id of target node>"}
     * response: {"id" : "<queried nodes id>", "nodes" : "<compact node info>"}
     */
    private static class FindNodeOperation extends Operation {

        static byte[] TEMPLATE =
                "d1:t2:TX1:y1:q1:q9:find_node1:ad2:id20:XXXXXXXXXXXXXXXXXXXX6:target20:XXXXXXXXXXXXXXXXXXXXee".
                getBytes(StandardCharsets.UTF_8);

        static final int TMPL_POS_TX = 6;
        static final int TMPL_POS_ID = 39;
        static final int TMPL_POS_TARGET = 70;

        HashId target;

        // nodes that will be queries
        List<RoutingTable.RemoteNode> nodes;

        // collection of the result nodes, merged from
        // response received from nodes
        List<RoutingTable.RemoteNode> result;

        HashId[] resultDistances;

        Consumer<List<RoutingTable.RemoteNode>> callback;

        /**
         *
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
         * review: remove extra copy of the id part
         * @param buffer buffer to populate
         * @param tx transaction id, only 2 low bytes will be used
         * @param target target node id to use in message
         */
        void populateSendBuffer(ByteBuffer buffer, byte[] tx, HashId target) {
            buffer.clear();
            buffer.put(TEMPLATE);
            buffer.put(TMPL_POS_TX, tx[0]).put(TMPL_POS_TX + 1, tx[1]);
            buffer.put(TMPL_POS_ID, node.id.getBytes());
            buffer.put(TMPL_POS_TARGET, target.getBytes());
            buffer.flip();
        }

        @Override
        boolean update() {

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


        /**
         * response: {"id" : "<queried nodes id>", "nodes" : "<compact node info>"}
         * @param address address of the sending party
         * @param response response received, only the internal response element
         * @return
         */
        @Override
        boolean receive(InetSocketAddress address, BEValue response) {

            recvCounter++;

            BEValue beQueried = response.dictionary.get("id");
            BEValue beNodes = response.dictionary.get("nodes");

            // that is ok for external nodes to simply drop nodes element
            if ((beNodes != null) && (beNodes.bString != null)) {
                List<RoutingTable.RemoteNode> remoteNodes = parseNodesCompactForm(beNodes.bString);
                // build closest list
                for (RoutingTable.RemoteNode rn: remoteNodes) {
                    node.routing._populateClosestList(target,  result, resultDistances, rn);
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
     * duplicate elements (nodes and peers), this will not happen if
     * only one node is queried (generic get_peers)
     */
    private static class GetPeersOperation extends Operation {

        static byte[] TEMPLATE =
                "d1:t2:TX1:y1:q1:q9:get_peers1:ad2:id20:XXXXXXXXXXXXXXXXXXXX9:info_hash20:XXXXXXXXXXXXXXXXXXXXee".
                getBytes(StandardCharsets.UTF_8);

        static final int TEMPLATE_POS_TX = 6;
        static final int TEMPLATE_POS_ID = 39;
        static final int TEMPLATE_POS_HASH = 73;

        HashId target;

        List<InetSocketAddress> foundPeers;
        List<RoutingTable.RemoteNode> foundNodes;

        List<RoutingTable.RemoteNode> nodes;

        BiConsumer<List<RoutingTable.RemoteNode>, List<InetSocketAddress>> callback;

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
        boolean update() {

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
        boolean receive(InetSocketAddress address, BEValue response) {
            /*
             * response could return peers or nodes to query further,
             * due to the spec formats are the following:
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

            // save active token for the answered node and
            // update state in the routing for the case of unknown node
            HashId queriedNodeId = HashId.wrap(beQueried.bString);
            for (int i = 0; i < nodes.size(); i++) {
                RoutingTable.RemoteNode n = nodes.get(i);
                if (n.id.equals(queriedNodeId)) {
                    n.token = beToken.bString;
                    node.routing.update(n);
                    break;
                }
            }

            if ((beNodes != null) && (beNodes.bString != null)) {
                // queried node knows more close nodes...
                List<RoutingTable.RemoteNode> remoteNodes = parseNodesCompactForm(beNodes.bString);
                foundNodes.addAll(remoteNodes);
            }

            if ((beValues != null) && beValues.isListNotEmpty()) {
                // queried node knows direct peers with the target hash (torrent)
                for(BEValue bePeer: beValues.list) {
                    InetSocketAddress isa = parsePeerCompactForm(bePeer.bString);
                    if (isa != null) {
                        foundPeers.add(isa);
                    }
                }
            }

            // end operation in place (could be done in update after timeout)
            if (nodes.size() <= recvCounter) {
                callback.accept(foundNodes, foundPeers);
                return OPERATION_FINISH;
            } else {
                return OPERATION_RERUN;
            }
        }
    }

    /**
     * Complex operation that runs bootstrapping procedure
     * on local node startup
     * cases:
     * 1. fresh start that needs neighbours search
     * 2. reload that must only publish itself (search new neighbours too??)
     */
    public static class BootstrapOperation extends Operation {

        static final int MAX_ITERATIONS = 100;

        // tracks nodes we have sent request too
        Set<RoutingTable.RemoteNode> queried;

        // nodes that are being queried on the current iteration
        List<RoutingTable.RemoteNode> nodes;

        // nodes received and to be queried with the next wave
        List<RoutingTable.RemoteNode> nextNodes;
        HashId[] nextNodesDistances;

        // tracks iterations while searching for nearest nodes nodes -> nodes -> nodes -> ..
        int iterationCounter;

        boolean callbackCalled;


        Consumer<Boolean> extCallback;

        public BootstrapOperation(Node _node) {
            super(_node);
            queried = new HashSet<>();
            nextNodes = new ArrayList<>(RoutingTable.Bucket.K);
            nextNodesDistances = new HashId[RoutingTable.Bucket.K];
        }

        public BootstrapOperation(Node _node, Consumer<Boolean> _extCallback) {
            this(_node);
            extCallback = _extCallback;
        }

        int state = 0; // 0 START, 1 NODES,

        @Override
        boolean update() {
            // global timeout protection
            if (timestamp + timeout < System.currentTimeMillis())
            {
                if (extCallback != null) {
                    extCallback.accept(node.routing.hasAliveNodes());
                }
                return OPERATION_FINISH;
            }

            if (state == 0 /*START*/) {
                // query routing table for closest nodes known,
                // at least one node is required
                nodes = node.routing.findClosestNodes(node.id);
                if (nodes.isEmpty()) {
                    // nowhere to search, end bootstrapping
                    if (extCallback != null) {
                        extCallback.accept(node.routing.hasAliveNodes());
                    }
                    return OPERATION_FINISH;
                }

                queried.addAll(nodes);

                // run search for our own hash id
                node.addOperation(new FindNodeOperation(node, nodes, node.id, rns -> {
                    callbackCalled = true;
                    nextNodes = rns;
                }));

                state = 1;

                return OPERATION_RERUN;
            }



            if (state == 1 /*NODES*/) {

                // wait for the result of the current wave
                if (!callbackCalled) {
                    return OPERATION_RERUN;
                }
                callbackCalled = false;

                // increment number of waves
                iterationCounter++;
                System.out.println("ITERATION: " + iterationCounter);

                // nodes are independent and could return
                // nodes we have already queried, throw such nodes out
                nextNodes.removeAll(queried);

                // insert all new nodes into routing table
                for (RoutingTable.RemoteNode rNode: nodes) {
                    node.routing.insert(rNode);
                }

                // have we reached the end of iterations?
                if (nextNodes.isEmpty() || (MAX_ITERATIONS < iterationCounter)) {
                    // ok, let's use the last list of nodes
                    state = 2;
                    return OPERATION_RERUN;
                }

                // track all queried (or errors)
                queried.addAll(nextNodes);

                // run next wave
                nodes = nextNodes;
                nextNodes = null;
                node.addOperation(new FindNodeOperation(node, nodes, node.id, rns -> {
                    callbackCalled = true;
                    nextNodes = rns;
                }));

                return OPERATION_RERUN;
            }

            if (state == 2 /*ANNOUNCE*/) {
                // state == 2 { ANNOUNCE to routing.getClosest()}

                // just return for now without ANNOUNCE
                if (extCallback != null) {
                    extCallback.accept(node.routing.hasAliveNodes());
                }
                return OPERATION_FINISH;
            }

            return OPERATION_RERUN;
        }


        @Override
        boolean receive(InetSocketAddress address, BEValue response) {
            // this operation doesn't really receive anything
            return false;
        }
    }

    /**
     * Complex operation that searches for peers that stores
     * any specific torrent (hash)
     */
    public  static class FindTorrentPeersOperation extends Operation {

        static final int MAX_ITERATIONS = 100;
        static final int MAX_NODES_FOR_ITERATION = 16;

        HashId target;

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

        boolean callbackCalled;

        Consumer<Set<InetSocketAddress>> extCallback;

        List<RoutingTable.RemoteNode> nodesDistinct;
        HashId[] nodesDistinctDistances;

        public FindTorrentPeersOperation(Node _node, HashId _target, Consumer<Set<InetSocketAddress>> _extCallback) {
            super(_node);
            target = _target;
            extCallback = _extCallback;
            queried = new HashSet<>();
            peers = new HashSet<>();

            nodesDistinct = new ArrayList<>(MAX_NODES_FOR_ITERATION);
            nodesDistinctDistances = new HashId[MAX_NODES_FOR_ITERATION];
        }

        int state = 0; // 0 START, 1 WAVES,

        @Override
        boolean update() {
            // global timeout protection
            if (timestamp + timeout < System.currentTimeMillis())
            {
                if (extCallback != null) {
                    extCallback.accept(peers);
                }
                return OPERATION_FINISH;
            }

            if (state == 0 /*START*/) {
                // query routing table for closest nodes known
                List<RoutingTable.RemoteNode> nodes = node.routing.findClosestNodes(target);
                if (nodes.isEmpty()) {
                    // nowhere to search, end search
                    if (extCallback != null) {
                        extCallback.accept(peers);
                    }
                    return OPERATION_FINISH;
                }

                queried.addAll(nodes);

                // run search for target torrent
                node.addOperation(new GetPeersOperation(node, nodes, target, (n, p) -> {
                    callbackCalled = true;
                    nodesFound = n;
                    peersFound = p;
                }));

                state = 1;
            }



            if (state == 1 /*NODES*/) {

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
                // only closest are left and duplicates are removed
                nodesDistinct.clear();
                for (RoutingTable.RemoteNode rNode: nodesFound) {
                    node.routing._populateClosestList(target, nodesDistinct, nodesDistinctDistances, rNode);
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

                // run next wave, this will insert inserted nodes into the routing
                node.addOperation(new GetPeersOperation(node, nodesDistinct, target, (n, p) -> {
                    callbackCalled = true;
                    nodesFound = n;
                    peersFound = p;
                }));
                return OPERATION_RERUN;
            }

            return OPERATION_RERUN;
        }

        @Override
        boolean receive(InetSocketAddress address, BEValue response) {
            // this operation doesn't really receive anything
            return false;
        }
    }
}
