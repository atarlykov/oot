package oot.tracker;

import oot.dht.HashId;
import oot.Torrent;

/**
 * HTTP tracker, just request http processor to handle announce requests
 */
public class HttpTracker extends Tracker {
    /**
     * ref to the parent processor
     */
    final HttpTrackerProcessor processor;
    /**
     * base tracker url
     */
    final String url;


    public HttpTracker(HashId _clientId, Torrent _torrent, HttpTrackerProcessor _processor, String _url) {
        super(_clientId, _torrent);
        processor = _processor;
        url = _url;
    }

    @Override
    public void announce(AnnounceEvent event, AnnounceCallback cb) {
        processor.process(this, event, cb);
    }
}
