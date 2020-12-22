package oot.tracker;

import oot.dht.HashId;
import oot.poc.Torrent;

import java.net.InetSocketAddress;
import java.util.Set;


/**
 * base class for all trackers
 */
public abstract class Tracker {
    /**
     * default update period to torrent
     */
    public static long DEF_UPDATE_TIMEOUT   = 15 * 60 * 1000;

    /**
     * call for most operations
     */
    public interface AnnounceCallback {
        /**
         * @param success false if can't connect, etc.
         * @param peers peers found
         */
        void call(boolean success, Set<InetSocketAddress> peers);
    }

    /**
     * type of events for announce requests to trackers
     */
    public enum AnnounceEvent {
        // default/update
        NONE(0),
        //torrent completed
        COMPLETED(1),
        // download started
        STARTED(2),
        // download stopped
        STOPPED(3);
        /**
         * code of the event (as in the spec)
         */
        int value;

        AnnounceEvent(int _value) {
            value = _value;
        }
    }

    /**
     * id of the local client (peer) to be used while communicating with a tracker
     */
    protected HashId clientId;
    /**
     * ref to the torrent we are working for
     */
    protected Torrent torrent;

    /**
     * allowed update period
     */
    long updatePeriod;
    /**
     * time of last update (peer search) operation
     */
    long updateLastTime;


    /**
     * number of seeder/leechers returned by the last call to tracker
     */
    int seeders;
    int leechers;


    /**
     * @param _clientId client id
     * @param _torrent information about torrent
     */
    protected Tracker(HashId _clientId, Torrent _torrent)
    {
        clientId = _clientId;
        torrent = _torrent;
        updatePeriod = DEF_UPDATE_TIMEOUT;
    }

    /**
     * @return current update period in ms if defined
     */
    long getUpdatePeriod() {
        return updatePeriod;
    }

    long getUpdateLastTime() {
        return updateLastTime;
    }

    boolean isReadyToUpdate() {
        return System.currentTimeMillis() < updateLastTime + updatePeriod;
    }

    /**
     * method that could be used to announce to this tracker
     * @param event type of announce
     * @param cb callback
     */
    public abstract void announce(AnnounceEvent event, AnnounceCallback cb);

    /**
     * api wrapper around generic announce implementation
     * @param cb callback to be called on result
     */
    public void update(AnnounceCallback cb)
    {
        updateLastTime = System.currentTimeMillis();
        announce(AnnounceEvent.NONE, cb);
    }

    /**
     * api wrapper around generic announce implementation
     * @param cb callback to be called on result
     */
    public void updateIfReady(AnnounceCallback cb)
    {
        if (isReadyToUpdate()) {
            announce(AnnounceEvent.NONE, cb);
        }
    }

    /**
     * api wrapper around generic announce implementation
     * @param cb callback to be called on result
     */
    public void started(AnnounceCallback cb) {
        announce(AnnounceEvent.STARTED, cb);
    }

    /**
     * api wrapper around generic announce implementation
     */
    public void stopped() {
        announce(AnnounceEvent.STOPPED, null);
    }

    /**
     * api wrapper around generic announce implementation
     */
    public void completed() {
        announce(AnnounceEvent.COMPLETED, null);
    }
}