package oot.tracker;

import oot.be.BEParser;
import oot.be.BEValue;
import oot.dht.HashId;
import oot.poc.Torrent;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Utility class to process requests to http trackers,
 * uses blocking HttpClient, could be optimized
 */
public class HttpTrackerProcessor {

    /**
     * executor for blocking http requests
     */
    ExecutorService executor = new ThreadPoolExecutor(4, 16, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>());


    /**
     * announce torrent to the specific tracker
     * @param tracker tracker (has ref to parent torrent)
     * @param event event
     */
    public void process(HttpTracker tracker, Tracker.AnnounceEvent event, Tracker.AnnounceCallback cb)
    {
        // HTTP(S) tracker, handle via executor, could be optimized
        String query = buildAnnounceUrl(tracker.torrent, tracker.url);
        CompletableFuture.supplyAsync(
                () -> httpAnnounce(query), executor)
                .thenApply(HttpTrackerProcessor::httpAnnounceParseResponse)
                .thenAccept(peers ->  cb.call(true, peers) );
    }

    /**
     * encodes byte array to be sent as http requests parameter (url encode),
     * simply encodes all symbols including the allowed ones
     * @param data data to be encoded
     * @return string safe representation
     */
    private static String encode(byte[] data)
    {
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
     * constructs http/https version of announce url for querying a server
     * @param torrent ref to torrent
     * @param url base url
     * @return complete url
     */
    private static String buildAnnounceUrl(Torrent torrent, String url)
    {
        long downloaded = torrent.downloaded.get();
        long left = torrent.metainfo.length - downloaded;
        left = (left < 0) ? 0 : left;
        return buildAnnounceUrl(
                torrent.getClientId(), 0, torrent.metainfo.infohash, url,
                downloaded, torrent.uploaded.get(), left);
    }
    /**
     * performs http(s) request to the specified url and
     * returns server response in BE form
     * @param query url to query
     * @return parsed server's response
     * @throws Exception on any IO error
     */
    private static Optional<BEValue> httpAnnounce(String query)
    {
        try {
            System.out.println(query);
            URL url = new URL(query);

            URLConnection urlConnection = url.openConnection();
            urlConnection.setConnectTimeout(5000);
            urlConnection.connect();

            //if (DEBUG) System.out.println("response code: " + ((HttpURLConnection)urlConnection).getResponseCode());

            if (((HttpURLConnection)urlConnection).getResponseCode() != 200) {
                return Optional.empty();
            }
            //((HttpURLConnection)urlConnection).getResponseMessage();

            byte[] data = urlConnection.getInputStream().readAllBytes();
            ByteBuffer b = ByteBuffer.wrap(data);
            BEParser parser = new BEParser();
            BEValue response = parser.parse(b);
            return Optional.of(response);
        } catch (IOException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    /**
     * parses tracker response as sent as answer to announce request
     * @param oData parsed binary encoded representation
     * @return not null set with unique peers
     */
    static private Set<InetSocketAddress> httpAnnounceParseResponse(Optional<BEValue> oData)
    {
        if (oData.isEmpty()) {
            return Collections.emptySet();
        }
        BEValue data = oData.get();
        if (!data.isDict() || !data.isDictNotEmpty()) {
            return Collections.emptySet();
        }
        BEValue beReason = data.dictionary.get("failure reason");
        if (beReason != null) {
            String reason = beReason.getBStringAsString();
            return Collections.emptySet();
        }

        // todo: support later
        //data.dictionary.get("interval");

        Set<InetSocketAddress> result = new HashSet<>();

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
                        result.add(isa);
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
                    result.add(isa);
                } catch (UnknownHostException ignored) {
                }
            }
            return result;
        }

        return Collections.emptySet();
    }

    /**
     * parses binary ip4 address format, 4 bytes address and 2 bytes port as big-endian,
     * @param buffer buffer with data, remaining size must be of length 6 at least
     * @return parsed address or null if something missing or wrong
     */
    InetSocketAddress parseIPv4Address(ByteBuffer buffer)
    {
        byte[] addr = new byte[4];
        buffer.get(addr);
        int port = buffer.getShort() & 0xFFFF;

        try {
            return new InetSocketAddress(Inet4Address.getByAddress(addr), port);
        } catch (UnknownHostException e) {
            // that shouldn't be as we don't resolve address
            return null;
        }
    }

    /**
     * parses binary ip6 address format, 16 bytes address and 2 bytes port as big-endian,
     * @param buffer buffer with data, remaining size must be of length 18 at least
     * @return parsed address or null if something missing or wrong
     */
    InetSocketAddress parseIPv6Address(ByteBuffer buffer)
    {
        byte[] addr = new byte[16];
        buffer.get(addr);
        int port = buffer.getShort() & 0xFFFF;

        try {
            return new InetSocketAddress(Inet6Address.getByAddress(addr), port);
        } catch (UnknownHostException e) {
            // that shouldn't be as we don't resolve address
            return null;
        }
    }



}
