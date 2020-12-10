package oot;

import oot.be.BEParser;
import oot.be.BEValue;
import oot.dht.HashId;

import java.io.IOException;
import java.net.*;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Selector;
import java.util.*;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * handles trackers linked to a torrent (bep0003),
 * stores update periods, statuses and performs
 * actual announce requests
 */
public class Tracker {

    public static final int TIMEOUT_ANNOUNCE = 15 * 60 * 1000;
    public static final int TIMEOUT_SCRAPE = 15 * 60 * 1000;

    /**
     * ref to parent torrent
     */
    Torrent torrent;

    /**
     * list of urls for this tracker,
     * can contain main url and several backups (bep0003)
     */
    List<String> urls;
    /**
     * index of the url to try next
     */
    int urlIndex;

    /**
     * time of last announce and scrape requests
     */
    long timeLastAnnounce;
    long timeLastScrape;

    /**
     * interval as returned by active tracker from the list,
     * zero if undefined
     */
    int interval;
    int leechers;
    int seeders;

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
    InetSocketAddress udpAddress;

    /**
     * indicates that this instance is being processed
     * at the moment, need to restrict double submissions
     */
    volatile boolean updating;

    // retransmission 15*2**n  0..8 + re-request cId


    String getCurrentUrl() {
        return urls.get(urlIndex);
    }

    // todo synch
    void switchToNextUrl() {
        urlIndex = (urlIndex + 1) % urls.size();
        interval = 0;
    }

    /*
       1. connect (tx)
       2. announce (tx)
       3. scrape

     */
    void x() {
        HttpClient client = HttpClient.newHttpClient();
        //client.sendAsync()


        //HttpClient.newBuilder().executor();
    }


    public Tracker(Torrent _torrent, List<String> _urls) {
        torrent = _torrent;

        urls = new ArrayList<>();
        if (_urls != null) {
            for (String url: _urls) {
                urls.add(url.toLowerCase());
            }
        }

        urlIndex = 0;
    }

    /**
     * perform announce call to this tracker
     */
    private void announce()
    {
//        String url = urls.get(urlIndex);
//        if (url.startsWith("http://") || url.startsWith("https://")) {
//            try {
//
//                String query = buildAnnounceUrl(
//                        torrent.getClient().id, 0, torrent.metainfo.getInfohash(), url,
//                        /*torrent.downloaded, torrent.uploaded, torrent.left*/
//                        0, 0, torrent.metainfo.length);
//                BEValue beValue = announce(query);
//                Set<Peer> peers = parseAnnounceResponse(beValue);
//                torrent.addPeers(peers);
//            } catch (Exception e) {
//                System.out.println(e.getMessage());
//                urlIndex = (urlIndex + 1) % urls.size();
//            }
//        }


        /*
        try {
            //Set<Peer> peers = Set.of(new Peer(new InetSocketAddress(Inet4Address.getByAddress(new byte[] {(byte)178, (byte)120, (byte)111, (byte)12}), 59121)));
            Set<Peer> peers = Set.of(new Peer(new InetSocketAddress(Inet4Address.getByAddress(new byte[] {(byte)127, (byte)0, (byte)0, (byte)1}), 42452)));
            torrent.addPeers(peers);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        */
    }

    /**
     * encodes byte array to be sent as http requests parameter (url encode),
     * simply encodes all symbols including the allowed ones
     * @param data data to be encoded
     * @return string safe representation
     */
    private static String encode(byte[] data) {
        StringBuilder builder = new StringBuilder(data.length * 3);
        for (byte b : data) {
            builder.append('%');
            builder.append(Character.forDigit((b >> 4) & 0x0F, 16));
            builder.append(Character.forDigit(b & 0x0F, 16));
        }
        return builder.toString();
    }

    /**
     * constructs http/https version of announce url for querying a server
     * @param clientId unique id ot he client (20 bits SHA1)
     * @param port port client is listening on
     * @param infohash 20 bits SHA1 of the torrent
     * @param announceUrl base url prefix
     * @param downloaded total number os bytes downloaded
     * @param uploaded total number of bytes uploaded
     * @param left number of bytes left to have 100% of the torrent
     * @return string representation of url
     */
    private static String buildAnnounceUrl(
            HashId clientId, int port, HashId infohash, String announceUrl,
            long downloaded, long uploaded, long left)
    {
        StringBuilder query = new StringBuilder();
        query.append(announceUrl)
                .append("?info_hash=").append(encode(infohash.getBytes()))
                .append("&peer_id=").append(encode(clientId.getBytes()))
                .append("&compact=").append(1)
                .append("&port=").append(port)
                .append("&uploaded=").append(uploaded)
                .append("&downloaded=").append(downloaded)
                .append("&left=").append(left);
        return query.toString();
    }

    String buildAnnounceUrl(String url)
    {
        return buildAnnounceUrl(
                torrent.getClient().id, 0, torrent.metainfo.infohash, url,
                /*torrent.downloaded, torrent.uploaded, torrent.left*/
                0, 0, torrent.metainfo.length);
    }

}
