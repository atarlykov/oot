package oot.tracker;

import oot.dht.HashId;
import oot.poc.Torrent;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * factory for standard trackers (https and udp)
 */
public class StandardTrackerFactory extends TrackerFactory
{
    // debug switch
    private static final boolean DEBUG = true;

    /**
     * processor of udp announces
     */
    UdpTrackerProcessor udpProcessor;
    /**
     * processor of http announces
     */
    HttpTrackerProcessor httpProcessor;

    /**
     * id of the local client
     */
    HashId clintId;

    /**
     * allowed constructor
     * @param clintId client id
     */
    public StandardTrackerFactory(HashId clintId) {
        this.clintId = clintId;

        try {
            udpProcessor = new UdpTrackerProcessor(clintId);
            udpProcessor.start();
        } catch (IOException e) {
            udpProcessor = null;
        }

        httpProcessor = new HttpTrackerProcessor();
    }


    @Override
    public List<Tracker> create(Torrent torrent)
    {
        List<Tracker> result = new ArrayList<>();

        List<List<String>> trackers = torrent.metainfo.trackers;
        for (List<String> urls: trackers) {
            if (urls.isEmpty()) {
                continue;
            }
            create(result, torrent, urls);
        }

        return result;
    }

    /**
     * populate result list with instances of trackers based on
     * main and slave torrent urls
     * @param torrent base torrent
     * @param urls urls of the main and (possibly) slave torrents
     */
    protected void create(List<Tracker> result, Torrent torrent, List<String> urls)
    {
        if (1 < urls.size()) {
            // create composite
            System.out.println("todo: composite url");
            return;
        }

        Tracker tracker = create(torrent, urls.get(0));
        if (tracker != null) {
            result.add(tracker);
        }
    }

    /**
     * creates instance of Tracker based on the url provided
     * @param torrent base torrent
     * @param url url
     */
    protected Tracker create(Torrent torrent, String url)
    {
        if (url.startsWith("http://") || url.startsWith("https://"))
        {
            HttpTracker tracker = new HttpTracker(clintId, torrent, httpProcessor, url);
            return tracker;
        }
        else if (url.startsWith("udp://")) {
            InetSocketAddress address = getUdpAddress(url);
            if (address == null) {
                return null;
            }
            UdpTracker tracker = new UdpTracker(clintId, torrent, udpProcessor, address);
            return tracker;
        }
        else if (DEBUG) {
            System.out.println("StandardTrackerFactory unknown url: " + url + " skipping");
        }
        return null;
    }

    /**
     * @param url url to parse
     * @return address or null of can't parse or resolve
     */
    protected InetSocketAddress getUdpAddress(String url)
    {
        // udp://1.2.3.4:port/announce
        // udp://[address]:port/announce
        // udp://ip[v]6.xxxxx:port/announce
        // we ignore 'announce' part

        url = url.substring(6);

        // this handles ipv6 in machine form and others
        int index = url.lastIndexOf(':');
        if (index == -1) {
            return null;
        }

        String addr = url.substring(0, index);
        int port = port(url, index + 1);

        try {
            return new InetSocketAddress(InetAddress.getByName(addr), port);
        } catch (Exception e) {
            if (DEBUG) {
                e.printStackTrace();
            }
            return null;
        }
    }

    /**
     * extracts port number from the string
     * @param url base string to extract from
     * @param index starting index of the port part
     * @return port value
     */
    private int port(String url, int index) {
        int port = 0;
        while ((index < url.length()) && Character.isDigit(url.charAt(index))) {
            port = port * 10 + Character.digit(url.charAt(index), 10);
            index++;
        }
        return port;
    }


}
