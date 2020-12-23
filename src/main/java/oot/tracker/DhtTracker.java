package oot.tracker;

import oot.dht.Node;
import oot.Torrent;

/**
 * wrapper to provide DHT peers to a torrent
 * via tracker interface
 */
public class DhtTracker extends Tracker {
    /**
     * ref to dht node
     */
    Node node;

    /**
     * @param _node ref to node
     * @param _torrent ref to parent torrent
     */
    public DhtTracker(Node _node, Torrent _torrent)
    {
        super(_node.getId(), _torrent);
        node = _node;
    }

    @Override
    public void announce(AnnounceEvent event, AnnounceCallback cb) {
        if (event != AnnounceEvent.NONE) {
            return;
        }
        if (cb == null) {
            return;
        }
        if (node.isBootstrapped()) {
            // this will be run in separate thread inside the node
            node.findPeers(torrent.getTorrentId(), addresses -> cb.call(true, addresses));
        }
    }
}
