package oot.dht;

import oot.be.BEValue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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
class PingPeersOperation extends PingOperation {

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
