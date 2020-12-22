package oot.tracker;

import oot.dht.HashId;
import oot.poc.Torrent;

public class HttpTracker extends Tracker {


    final String url;

    public HttpTracker(HashId _clientId, Torrent _torrent, HttpTrackerProcessor _processor, String _url) {
        super(_clientId, _torrent);
        url = _url;
    }

    @Override
    public void announce(AnnounceEvent event, AnnounceCallback cb) {
        
    }
}
