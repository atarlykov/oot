package oot.poc;

import oot.dht.HashId;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.BitSet;

/**
 * Service class used to produce and cache instances of peer messages
 * to not create them for each frequent message like request/piece
 */
public class StdPeerMessageCache {

    /**
     * max allowed size of the cache
     */
    private static final int CACHE_SIZE = 256;

    /**
     * cache of available instances
     */
    private final ArrayDeque<StdPeerMessage> cache = new ArrayDeque<>(CACHE_SIZE);

    /**
     * debug counter
     */
    public static int counter = 0;

    /**
     * @param type {@link StdPeerMessage#type}
     * @return instance of typed peer message to be used
     */
    private StdPeerMessage getInstance(byte type) {
        StdPeerMessage pm;
        synchronized (cache) {
            pm = cache.pollFirst();
        }
        if (pm == null) {
            pm = new StdPeerMessage(type);
            counter++;
        }
        pm.type = type;
        return pm;
    }

    /**
     * returns peer message to cache for future use
     * @param pm message to return
     */
    public void release(StdPeerMessage pm)
    {
        assert cache.contains(pm) : "the same instance released twice";

        pm.block = null;
        pm.params = null;
        // todo: remove sync?
        synchronized (cache) {
            if (cache.size() < CACHE_SIZE) {
                cache.offerFirst(pm);
            }
        }
    }

    /*
     * next methods simply work as a facade and
     * return messages of the appropriate type
     */

    public StdPeerMessage choke() {
        return getInstance(StdPeerMessage.CHOKE);
    }

    public StdPeerMessage unchoke() {
        return getInstance(StdPeerMessage.UNCHOKE);
    }

    public StdPeerMessage interested() {
        return getInstance(StdPeerMessage.INTERESTED);
    }

    public StdPeerMessage notInterested() {
        return getInstance(StdPeerMessage.NOT_INTERESTED);
    }

    public StdPeerMessage have(int index) {
        StdPeerMessage pm = getInstance(StdPeerMessage.HAVE);
        pm.index = index;
        return pm;
    }

    public StdPeerMessage request(int index, int begin, int length) {
        StdPeerMessage pm = getInstance(StdPeerMessage.REQUEST);
        pm.index = index;
        pm.begin = begin;
        pm.length = length;
        return pm;
    }

    public StdPeerMessage cancel(int index, int begin, int length) {
        StdPeerMessage pm = getInstance(StdPeerMessage.CANCEL);
        pm.index = index;
        pm.begin = begin;
        pm.length = length;
        return pm;
    }

    public StdPeerMessage piece(int index, int begin, int length, ByteBuffer buffer, Object param) {
        StdPeerMessage pm = getInstance(StdPeerMessage.PIECE);
        pm.index = index;
        pm.begin = begin;
        pm.length = length;
        pm.block = buffer;
        pm.params = param;
        return pm;
    }

    public StdPeerMessage bitfield(int pieces, BitSet state) {
        StdPeerMessage pm = getInstance(StdPeerMessage.BITFIELD);
        pm.index = pieces;
        pm.params = new Object[1];
        pm.params = state;

        return pm;
    }

    public StdPeerMessage port(int port) {
        StdPeerMessage pm = getInstance(StdPeerMessage.PORT);
        pm.index = port;
        return pm;
    }

    public StdPeerMessage keepalive() {
        return getInstance(StdPeerMessage.KEEPALIVE);
    }

    public StdPeerMessage handshake(HashId torrent, HashId peer)
    {
        StdPeerMessage pm = getInstance(StdPeerMessage.HANDSHAKE);
        HashId[] params = new HashId[2];
        params[0] = torrent;
        params[1] = peer;
        pm.params = params;
        return pm;
    }

}
