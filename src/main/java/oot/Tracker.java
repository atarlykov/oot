package oot;

import oot.be.BEParser;
import oot.be.BEValue;
import oot.dht.HashId;

import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;

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


    void announce() {
/*
        String url = urls.get(urlIndex);
        if (url.startsWith("http://") || url.startsWith("https://")) {
            try {
                BEValue beValue = announce(torrent.getClient().id, 0, torrent.metainfo.getInfohash(), url, torrent.downloaded, torrent.uploaded, torrent.left);
                Set<Peer> peers = parseAnnounceResponse(beValue);
                torrent.addPeers(peers);
                System.out.println("  " + url + "    peers:" + peers.size());
            } catch (Exception e) {
                urlIndex = (urlIndex + 1) % urls.size();
                System.out.println("  " + url + "    " + e.getMessage());
            }
        }

*/

        try {
            //Set<Peer> peers = Set.of(new Peer(new InetSocketAddress(Inet4Address.getByAddress(new byte[] {(byte)178, (byte)120, (byte)111, (byte)12}), 59121)));
            Set<Peer> peers = Set.of(new Peer(new InetSocketAddress(Inet4Address.getByAddress(new byte[] {(byte)127, (byte)0, (byte)0, (byte)1}), 42452)));
            torrent.addPeers(peers);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }


    }

    /**
     * encodes byte array to be sent as http requests parameter (url encode)
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

    private static BEValue announce(HashId clientId, int port, HashId infohash, String announceUrl,
                        long downloaded, long uploaded, long left)
            throws Exception
    {
        String query = buildAnnounceUrl(clientId, port, infohash, announceUrl, downloaded, uploaded, left);
        URL url = new URL(query);

        URLConnection urlConnection = url.openConnection();
        urlConnection.setConnectTimeout(5000);
        urlConnection.connect();

        if (((HttpURLConnection)urlConnection).getResponseCode() != 200) {

        }
        //((HttpURLConnection)urlConnection).getResponseMessage();

        byte[] data = urlConnection.getInputStream().readAllBytes();
        ByteBuffer b = ByteBuffer.wrap(data);
        BEParser parser = new BEParser();
        BEValue response = parser.parse(b);
        return response;
    }

    /**
     * parses tracker reply as sent as answer to announce request
     * @param data parsed binary encoded representation
     * @return not null set with unique peers
     */
    public Set<Peer> parseAnnounceResponse(BEValue data) {
        if ((data == null) || !data.isDict() || !data.isDictNotEmpty()) {
            Collections.emptySet();
        }
        BEValue beReason = data.dictionary.get("failure reason");
        if (beReason != null) {
            String reason = beReason.getBStringAsString();
            return Collections.emptySet();
        }

        data.dictionary.get("interval");

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
