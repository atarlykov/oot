package oot.tracker;

import oot.dht.Node;
import oot.Torrent;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

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
     * ref to callback to be notified with peers discovered,
     * could be registered dynamically
     */
    protected volatile Consumer<Collection<InetSocketAddress>> peersCallback;

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

    @Override
    public void setPeersCallback(Consumer<Collection<InetSocketAddress>> callback) {
        throw new RuntimeException("not implemented for DHTTF");
    }
}
