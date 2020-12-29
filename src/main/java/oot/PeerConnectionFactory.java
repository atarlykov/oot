package oot;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;


/**
 * base class for all connections' related tasks,
 * to be used from #Torrent class to open outgoing connections
 * and from connections' accepting code to handle connecting and
 * provide connection with necessary data
 */
abstract class PeerConnectionFactory {

    /**
     * *ref* to the selector used, hardly coupled with a threading model.
     * Could be a separate selector per torrent to simplify distribution of torrents
     * between thread.
     */
    protected Selector selector;

    /**
     * implementation of the interface to be used to get torrent
     */
    protected StdHandshakePeerConnection.TorrentProvider torrentProvider;

    /**
     * @param _selector selector
     * @param _tProvider provider
     */
    public PeerConnectionFactory(Selector _selector, StdHandshakePeerConnection.TorrentProvider _tProvider) {
        selector = _selector;
        torrentProvider = _tProvider;
    }

    /**
     * creates new peer connection's implementation that reflects incoming connection
     * and will perform all necessary tasks to establish app level connection,
     * including registering of the channel within the selector
     *
     * @param _channel opened socket channel
     * @param _peer peer to we have connection with
     * @return selection key with connection as the parameter
     * @throws IOException if connection could not be registered with the selector
     */
    public abstract SelectionKey acceptConnection(
            SocketChannel _channel, Peer _peer) throws IOException;

    /**
     * Opens new connection to the peer specified, registers it within the selector,
     * mostly assumed to be used from a torrent handling strategy when it needs a new connection
     * @param _peer peer to connect to
     * @param _torrent base torrent for the connection
     * @return connection object
     */
    public abstract PeerConnection openConnection(Peer _peer, Torrent _torrent);

    /**
     * called to close a connection opened earlier
     * @param pc connection
     */
    public abstract void closeConnection(PeerConnection pc);
}
