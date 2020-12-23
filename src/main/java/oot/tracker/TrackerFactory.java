package oot.tracker;

import oot.Torrent;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * integration class for producing trackers
 * for any specific torrent
 */
public abstract class TrackerFactory {
    /**
     * registers callback to be called when peers are discovered
     * via trackers, controlled by this factory
     * @param callback callback
     */
    public abstract void setPeersCallback(Consumer<Collection<InetSocketAddress>> callback);

    /**
     * creates tracker's implementations to for the specified torrent
     * @param torrent ref to torrent
     * @return not null list of trackers for the torrent
     */
    public abstract List<Tracker> create(Torrent torrent);

}
