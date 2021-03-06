package oot;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.BitSet;

/**
 * Service class used to produce and cache instances of peer messages
 * to not create them for each frequent message like request/piece
 */
public class PeerMessageCache {

    /**
     * max allowed size of the cache
     */
    private static final int CACHE_SIZE = 256;

    /**
     * cache of available instances
     */
    private final ArrayDeque<PeerMessage> cache = new ArrayDeque<>(CACHE_SIZE);

    /**
     * debug counter
     */
    public static int counter = 0;

    /**
     * @param type {@link PeerMessage#type}
     * @return instance of typed peer message to be used
     */
    private PeerMessage getInstance(byte type) {
        PeerMessage pm;
        synchronized (cache) {
            pm = cache.pollFirst();
        }
        if (pm == null) {
            pm = new PeerMessage(type);
            counter++;
        }
        pm.type = type;
        return pm;
    }

    /**
     * returns peer message to cache for future use
     * @param pm message to return
     */
    public void release(PeerMessage pm)
    {
        assert cache.contains(pm) : "the same instance released twice";

        pm.block = null;
        pm.pieces = null;
        synchronized (cache) {
            if (cache.size() < CACHE_SIZE) {
                cache.offerFirst(pm);
            }
        }
    }

    /*
     * next methods simply work as a facade and
     * return messages of appropriate type
     */


    public PeerMessage choke() {
        return getInstance(PeerMessage.CHOKE);
    }

    public PeerMessage unchoke() {
        return getInstance(PeerMessage.UNCHOKE);
    }

    public PeerMessage interested() {
        return getInstance(PeerMessage.INTERESTED);
    }

    public PeerMessage notInterested() {
        return getInstance(PeerMessage.NOT_INTERESTED);
    }

    public PeerMessage have(int index) {
        PeerMessage pm = getInstance(PeerMessage.HAVE);
        pm.index = index;
        return pm;
    }

    public PeerMessage request(int index, int begin, int length) {
        PeerMessage pm = getInstance(PeerMessage.REQUEST);
        pm.index = index;
        pm.begin = begin;
        pm.length = length;
        return pm;
    }

    public PeerMessage cancel(int index, int begin, int length) {
        PeerMessage pm = getInstance(PeerMessage.CANCEL);
        pm.index = index;
        pm.begin = begin;
        pm.length = length;
        return pm;
    }

    public PeerMessage piece(int index, int begin, int length, ByteBuffer block) {
        PeerMessage pm = getInstance(PeerMessage.PIECE);
        pm.index = index;
        pm.begin = begin;
        pm.length = length;
        pm.block = block;
        return pm;
    }

    public PeerMessage bitfield(int pieces, BitSet state) {
        PeerMessage pm = getInstance(PeerMessage.BITFIELD);
        pm.index = pieces;
        pm.pieces = state;
        return pm;
    }

    public PeerMessage port(int port) {
        PeerMessage pm = getInstance(PeerMessage.PORT);
        pm.index = port;
        return pm;
    }

    public PeerMessage keepalive() {
        return getInstance(PeerMessage.KEEPALIVE);
    }
}
