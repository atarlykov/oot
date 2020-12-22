package oot.tracker;

import oot.dht.HashId;
import oot.poc.Torrent;

import java.net.InetSocketAddress;

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
     *
     */
    InetSocketAddress address;

    public UdpTracker(HashId _clientId, Torrent _torrent, UdpTrackerProcessor _processor, InetSocketAddress _address) {
        super(_clientId, _torrent);
        address = _address;
    }

    @Override
    public void announce(AnnounceEvent event, AnnounceCallback cb) {

    }
}
