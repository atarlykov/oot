package oot.tracker;

import oot.dht.HashId;
import oot.Torrent;

import java.net.InetSocketAddress;

/**
 * UDP trackers support, redirect announces to udp processor
 */
public class UdpTracker extends Tracker {
    /**
     * last connection id received from a tracker (udp mode)
     */
    long udpConnectionId;
    /**
     * timestamp of the last connection id received
     */
    long udpConnectionTime;
    /**
     * base address to access
     */
    InetSocketAddress address;
    /**
     * ref to parent processor
     */
    UdpTrackerProcessor processor;


    public UdpTracker(HashId _clientId, Torrent _torrent, UdpTrackerProcessor _processor, InetSocketAddress _address)
    {
        super(_clientId, _torrent);
        processor = _processor;
        address = _address;
    }

    @Override
    public void announce(AnnounceEvent event, AnnounceCallback cb) {
        processor.process(this, event, cb);
    }
}
