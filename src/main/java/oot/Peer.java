package oot;

import oot.dht.HashId;

import java.net.InetSocketAddress;

/**
 * information about known peers that could have data for a torrent,
 * NOTE: not shared between torrents, each torrent must have a copy
 * to allow tracking of possible connections' errors
 */
public class Peer {
    /**
     * time period to allow subsequent connections to the same peer
     * after connecting error
     */
    private static final long RECONNECT_PERIOD_UNACCESSIBLE     = 1800_000L;
    /**
     * time period to allow subsequent connections to the same peer
     * after logic protocol error, data lost, code problem on our side, etc.
     */
    private static final long RECONNECT_PERIOD_PROTOCOL_ERROR   =   10_000L;
    /**
     * time period to allow subsequent connections to the same peer
     * after we have closed the connection, maybe we are interested if new data is available
     */
    private static final long RECONNECT_PERIOD_NORMAL           =  600_000L;

    /**
     * reasons of closing the last connection to this peer
     */
    public enum CloseReason {
        /**
         * was not able to connect to the peer
         */
        INACCESSIBLE,
        /**
         * there were some logic errors parsing protocol data
         */
        PROTOCOL_ERROR,
        /**
         * closed due to some decision on our side like
         * no more data we are interested in, etc.
         */
        NORMAL
    }

    /**
     * address of the remote side
     */
    InetSocketAddress address;
    /**
     * id of the peer received after the 1st (or last) handshake
     */
    HashId peerId;
    /**
     * reserved buts received after the 1st (or last) handshake
     */
    byte[] reserved;
    /**
     * DHT port if available and 0 of not/unknown
     */
    int dhtPort = 0;

    /**
     * timestamp when connection was closed
     */
    private long connectionCloseTimestamp;
    /**
     * reason of the last connection close
     */
    private CloseReason connectionCloseReason;
    /**
     * true if we know somehow that this peer
     * has all pieces of the torrent
     */
    private boolean completed;

    /**
     * allowed constructor
     * @param _address address of the peer
     */
    public Peer(InetSocketAddress _address) {
        this.address = _address;
    }

    /**
     * @return true if peer is ok to try connecting
     */
    public boolean isConnectionAllowed()
    {
        if (connectionCloseReason == null) {
            // new state, connections are allowed
            return true;
        }

        long period = switch (connectionCloseReason) {
            case INACCESSIBLE -> RECONNECT_PERIOD_UNACCESSIBLE;
            case PROTOCOL_ERROR -> RECONNECT_PERIOD_PROTOCOL_ERROR;
            case NORMAL         -> RECONNECT_PERIOD_NORMAL;
        };

        long now = System.currentTimeMillis();
        return connectionCloseTimestamp + period < now;
    }

    /**
     * resets "connection is closed" state
     */
    public void resetConnectionClosed() {
        connectionCloseReason = null;
    }

    /**
     * sets connection state to error or not
     * @param reason reason of the close
     */
    public void setConnectionClosed(CloseReason reason)
    {
        connectionCloseReason = reason;
        connectionCloseTimestamp = System.currentTimeMillis();
    }

    /**
     * @return true if there were connection error
     */
    public boolean isConnectionClosed() {
        return connectionCloseReason != null;
    }

    /**
     * @return true if this peer has all pieces of the parent torrent
     */
    public boolean isCompleted() {
        return completed;
    }

    /**
     * sets completion flag for this peer
     * @param completed true if completed
     */
    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Peer peer = (Peer) o;

        return address.equals(peer.address);
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }
}
