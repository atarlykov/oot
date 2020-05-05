package oot;

import java.net.InetSocketAddress;

public class Peer {

    private static final long CONNECTION_CHECK_PERIOD = 600_000;

    /**
     * address of the remote side
     */
    InetSocketAddress address;
    /**
     * DHT port if available and 0 of not/unknown
     */
    int dhtPort = 0;

    /**
     * indicates there was error on last attempt to connect
     * or connection was closed by our side
     */
    private volatile boolean connectionClosed;
    /**
     * timestamp when connection was closed
     */
    private volatile long connectionClosedTimestamp;

    public Peer(InetSocketAddress _address) {
        this.address = _address;
        connectionClosed = false;
    }

    /**
     * @return true if peer is ok to try connecting
     */
    public boolean isConnectionAllowed() {
        return !connectionClosed
                || (CONNECTION_CHECK_PERIOD < System.currentTimeMillis() - connectionClosedTimestamp);
    }

    /**
     * sets connection state to error or not
     * @param state state to set, true if there was connection error
     */
    public void setConnectionClosed(boolean state) {
        connectionClosed = state;
        if (connectionClosed) {
            connectionClosedTimestamp = System.currentTimeMillis();
        }
    }

    /**
     * @return true if there were connection error
     */
    public boolean isConnectionClosed() {
        return connectionClosed;
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
