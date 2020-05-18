package oot;

import oot.be.BEParser;
import oot.be.BEValue;
import oot.dht.HashId;

import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;

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
     * must be called periodically to perform tracker update,
     * internally controls update period to be not less than TIMEOUT_ANNOUNCE
     * updates and run in background in service threads controlled by Client
     */
    void update() {
        long now = System.currentTimeMillis();
        if (TIMEOUT_ANNOUNCE < now - timeLastAnnounce) {
            torrent.getClient().executor.submit((Runnable) this::announce);
            timeLastAnnounce = now;
        }

        if (TIMEOUT_SCRAPE < now - timeLastScrape) {
            timeLastScrape = now;
        }
    }

    /**
     * perform announce call to this tracker
     */
    private void announce()
    {
        String url = urls.get(urlIndex);
        if (url.startsWith("http://") || url.startsWith("https://")) {
            try {

                String query = buildAnnounceUrl(
                        torrent.getClient().id, 0, torrent.metainfo.getInfohash(), url,
                        /*torrent.downloaded, torrent.uploaded, torrent.left*/
                        0, 0, torrent.metainfo.length);
                BEValue beValue = announce(query);
                Set<Peer> peers = parseAnnounceResponse(beValue);
                torrent.addPeers(peers);
            } catch (Exception e) {
                System.out.println(e.getMessage());
                urlIndex = (urlIndex + 1) % urls.size();
            }
        }


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

    /**
     * performs blocking request to the specified url and
     * parses server response into BE representation
     * @param query url to query
     * @return parsed server's response
     * @throws Exception on any IO error
     */
    private static BEValue announce(String query)
            throws Exception
    {
        System.out.println(query);
        URL url = new URL(query);

        URLConnection urlConnection = url.openConnection();
        urlConnection.setConnectTimeout(5000);
        urlConnection.connect();

        System.out.println("response code: " + ((HttpURLConnection)urlConnection).getResponseCode());

        if (((HttpURLConnection)urlConnection).getResponseCode() != 200) {
            return null;
        }
        //((HttpURLConnection)urlConnection).getResponseMessage();

        byte[] data = urlConnection.getInputStream().readAllBytes();
        ByteBuffer b = ByteBuffer.wrap(data);
        BEParser parser = new BEParser();
        BEValue response = parser.parse(b);
        return response;
    }

    /**
     * parses tracker response as sent as answer to announce request
     * @param data parsed binary encoded representation
     * @return not null set with unique peers
     */
    public Set<Peer> parseAnnounceResponse(BEValue data)
    {
        if ((data == null) || !data.isDict() || !data.isDictNotEmpty()) {
            return Collections.emptySet();
        }
        BEValue beReason = data.dictionary.get("failure reason");
        if (beReason != null) {
            String reason = beReason.getBStringAsString();
            return Collections.emptySet();
        }

        // todo: support later
        //data.dictionary.get("interval");

        Set<Peer> result = new HashSet<>();

        BEValue peers = data.dictionary.get("peers");
        if (BEValue.isList(peers)) {
            // bep0003, this is list of dictionaries: [{peer id, ip, port}]
            for (int i = 0; i < peers.list.size(); i++) {
                BEValue bePeer = peers.list.get(i);
                if (!BEValue.isDict(bePeer)) {
                    continue;
                }
                BEValue peer_id = bePeer.dictionary.get("peer id");
                BEValue ip = bePeer.dictionary.get("ip");
                BEValue port = bePeer.dictionary.get("port");

                if (BEValue.isBString(ip) && BEValue.isBString(port)) {
                    try {
                        InetSocketAddress isa = new InetSocketAddress(
                                Inet4Address.getByName(ip.getBStringAsString()),
                                Integer.parseInt(port.getBStringAsString())
                        );
                        Peer peer = new Peer(isa);
                        result.add(peer);
                    } catch (UnknownHostException ignore) {
                    }
                }
            }
            return result;
        }

        if (BEValue.isBString(peers)) {
            // bep0023 compact representation ipv4 addressed "4+2|.."
            byte[] tmp = new byte[4];
            for (int i = 0; i < peers.bString.length / 6; i++) {
                try {
                    tmp[0] = peers.bString[i*6 + 0];
                    tmp[1] = peers.bString[i*6 + 1];
                    tmp[2] = peers.bString[i*6 + 2];
                    tmp[3] = peers.bString[i*6 + 3];

                    int port = (Byte.toUnsignedInt(peers.bString[i*6 + 4]) << 8)
                            | Byte.toUnsignedInt(peers.bString[i*6 + 5]);

                    InetSocketAddress isa = new InetSocketAddress( Inet4Address.getByAddress(tmp), port);
                    Peer peer = new Peer(isa);
                    result.add(peer);
                } catch (UnknownHostException ignored) {
                }
            }
            return result;
        }

        return Collections.emptySet();
    }


}
