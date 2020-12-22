package oot.tracker;

import oot.dht.HashId;
import oot.poc.Torrent;

import java.util.List;

/**
 * integration class for producing trackers
 * for any specific torrent
 */
public abstract class TrackerFactory {

    /**
     * creates tracker's implementations to for the specified torrent
     * @param torrent ref to torrent
     * @return not null list of trackers for the torrent
     */
    public abstract List<Tracker> create(Torrent torrent);

}
