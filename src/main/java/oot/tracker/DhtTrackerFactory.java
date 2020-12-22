package oot.tracker;

import oot.dht.Node;
import oot.poc.Torrent;

import java.util.List;

/**
 * factory to create DHT trackers for torrents,
 * wrapper to use DHT node as a tracker
 */
public class DhtTrackerFactory extends TrackerFactory
{
    /**
     * ref to dht node
     */
    Node node;

    /**
     * @param _node ref to node
     */
    public DhtTrackerFactory(Node _node) {
        node = _node;
    }

    @Override
    public List<Tracker> create(Torrent torrent) {
        return List.of(new DhtTracker(node, torrent));
    }
}
