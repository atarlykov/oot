package oot.dht;

import java.util.List;

/**
 * Complex operation to announce torrent being locally downloaded,
 * performs search for nodes closest to the hash of the torrent
 * and then announces to them
 * todo: include local node to peers
 */
class AnnounceTorrentOperation extends Operation
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
