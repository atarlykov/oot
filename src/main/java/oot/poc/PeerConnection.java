package oot.poc;

import oot.Torrent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Formatter;

/**
 *
 * todo: check for peer.xxx usages hare, move to upper level
 *
 *
 */
public abstract class PeerConnection
{
    // debug switch
    private static final boolean DEBUG = true;

    /**
     * length of time period to aggregate downloaded and uploaded bytes,
     * in milliseconds
     */
    public static final int STATISTICS_PERIOD_LENGTH = 1000;
    /**
     * number of periods to store for download/upload statistics
     */
    public static final int STATISTICS_PERIODS = 8;


    /**
     * ref to the parent peer,
     * could be removed as we need only address here (no peer's lifecycle here)
     */
    Peer peer;

    /**
     * ref to the selector this connection uses
     */
    Selector selector;

    /**
     * channel to communicate with remote peer
     */
    SocketChannel channel;
    /**
     * selection key, registered for notifications
     */
    SelectionKey sKey;

    /**
     * buffer used to send messages via channel,
     * default state is the following:
     *  - position points to start of the data ready to be sent
     *  - limit points after the end the prepared data
     *  - (pos, lim) are (0, 0 if buffer is empty
     */
    ByteBuffer sendBuffer;
    /**
     * buffer to write data when receiving from the channel
     * default state is the following (like send buffer):
     *  - position points to start of the data ready to be sent
     *  - limit points after the end the prepared data
     *  - (pos, lim) are (0, 0 if buffer is empty
     *
     * most received messages are small except for PIECE and BITFIELD,
     * but PIECE is the longest... so, only first half of the buffer
     * is used in normal mode, but when start of PIECE message is
     * received, buffer switches to use second half of te buffer too
     * to store linear PIECE
     *
     * all messages are: [4b length][1b type][(length - 1)b data]
     * piece message: [4 + 1 + 4 + 4 + PIECE_BLOCK_MAX_SIZE]
     *
     */
    ByteBuffer recvBuffer;


    // are we operating in extended mode when
    // whole receive buffer is used to receive tail
    // of a long message (PIECE)
    boolean extendedReceiveBufferMode = false;

    // number of data we must receive to have
    // the full long message (PIECE) in the buffer
    int extendedReceiveBufferModeTailSize = 0;

    int receiveBufferNormalLimit;
    int receiveBufferCompactionLimit;


    int sendBufferNormalLimit;
    int sendBufferCompactionLimit;
    boolean sendHasMoreData;

    /**
     * tracks connected status to allow
     * non-blocking connections establishing
     */
    public boolean connected = false;
    /**
     * timestamp of the connection event
     */
    public long timeConnected;



    /**
     * various per connection statistics
     */
    PeerConnectionStatistics rawStatistics;

    /**
     * download speed limit in bytes/sec or zero if unlimited,
     * this limit is dynamic and managed by parent torrent
     * based on global limits, per torrent limits and current
     * throughput of all connections,
     * used while requesting blocks from external peer
     */
    long speedLimitDownload;
    /**
     * upload speed limit in bytes/sec or zero if unlimited,
     * this limit is dynamic and managed by parent torrent
     * based on global limits, per torrent limits and current
     * throughput of all connections,
     *
     */
    long speedLimitUpload;


    /**
     * allowed constructor
     * @param _selector
     * @param _peer
     * @param _receiveBuffer
     * @param _receiveBufferNormalLimit
     * @param _receiveBufferCompactionLimit
     * @param _sendBuffer
     * @param _sendBufferNormalLimit
     * @param _sendBufferCompactionLimit
     */
    public PeerConnection(
            Selector _selector, Peer _peer,
            ByteBuffer _receiveBuffer,
            int _receiveBufferNormalLimit, int _receiveBufferCompactionLimit,
            ByteBuffer _sendBuffer,
            int _sendBufferNormalLimit, int _sendBufferCompactionLimit)
    {
        selector = _selector;
        peer = _peer;

        recvBuffer = _receiveBuffer;
        receiveBufferNormalLimit = _receiveBufferNormalLimit;
        receiveBufferCompactionLimit = _receiveBufferCompactionLimit;

        sendBuffer = _sendBuffer;
        sendBufferNormalLimit = _sendBufferNormalLimit;
        sendBufferCompactionLimit = _sendBufferCompactionLimit;

        rawStatistics = new PeerConnectionStatistics(STATISTICS_PERIODS, STATISTICS_PERIOD_LENGTH);

        // reset state to start negotiation
        reset();
    }


    void dump(Formatter formatter) {
    }

    /**
     * @param seconds number of seconds
     * @return average download speed for the specified number of seconds
     */
    long getDownloadSpeed(int seconds) {
        return rawStatistics.download.average(seconds);
    }

    /**
     * @param seconds number of seconds
     * @return average upload speed for the specified number of seconds
     */
    long getUploadSpeed(int seconds) {
        return rawStatistics.upload.average(seconds);
    }

    /**
     * integration method for parent torrent, allows to make
     * back call with parameters of blocks that should be downloaded
     * by this connection
     * NOTE: not very OOP, but remove extra objects' creation
     * @param index index of the piece that holds the block
     * @param position position in of the block to request / usually as (block index << 14)
     * @param length length of the block / usually fixed
     */
    abstract void enqueueBlockRequest(int index, int position, int length);

    /**
     * default lifecycle
     */
    void update()
    {
        // initiate / finish connect
        if (!connected) {
            try {
                connect();
            } catch (IOException e) {
                if (DEBUG) System.out.println("peer: " + peer.address + "  io exception, error [1], " + e.getMessage());
                // set connection error, trigger cleanup and notification
                close(Peer.CloseReason.INACCESSIBLE);
            }
        }
    }


    /**
     * resets state of this connection to allow
     * reconnects on errors
     */
    protected void reset()
    {
        // connection state
        connected = false;

        // buffers
        sendBuffer.position(0).limit(0);
        recvBuffer.position(0).limit(0);
        extendedReceiveBufferMode = false;

        rawStatistics.reset();
        speedLimitDownload = 0;
        speedLimitUpload = 0;
    }


    /**
     * called once after connection has been established
     */
    protected abstract void connectionEstablished();

    /**
     * performs connection to the external peer,
     * fires
     * @return true if connection established and false if it's still pending
     * @throws IOException if any
     */
    boolean connect() throws IOException
    {
        // connection established event,
        // this fires only once
        boolean established = false;

        if (channel == null) {
            // initiate connect
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            connected = channel.connect(peer.address);
            // could be true for local connections
            established = connected;
        }
        else if (!connected) {
            // channel was created but connection is still pending,
            // try to finish it
            connected = channel.finishConnect();
            established = connected;
        }

        if (established) {
            // ok, connection has been established,
            // initiate registration and other actions
            // (registration is internally synchronized)
            sKey = channel.register(selector, SelectionKey.OP_READ, this);
            // store time for connection management
            timeConnected = System.currentTimeMillis();

            // fire event for child classes
            connectionEstablished();
        }

        return connected;
    }


    /**
     * called when connection is closed
     * @param reason reason of the close
     */
    protected abstract void connectionClosed(Peer.CloseReason reason);

    /**
     * cancels selection key to stop receiving notifications
     * and closes the channel, calls {@link #connectionClosed(Peer.CloseReason)} to notify child class
     */
    public void close(Peer.CloseReason reason)
    {
        try {
            if (sKey != null) {
                sKey.cancel();
            }
            if (channel != null) {
                channel.close();
            }
        } catch (IOException ignored) {}

        connectionClosed(reason);
    }


    /**
     * called by {@link #receive()} method to process data inside
     * the receive buffer, buffer has the following state on enter:
     *  .position   - start of the data to process
     *  .limit      - end of the data received
     * on exit from the method, buffer state must be:
     *  .position   - somewhere before .limit, marking unprocessed data
     *  .limit      - as was at start (will be fixed to this state)
     *
     * @param rb reference to the receive buffer
     * @return size of the expected data to have a complete message in the buffer,
     * zero in case of buffer end on a message boundary
     * "-1" in case of any error (connection will be closed)
     */
    protected abstract int processReceiveBuffer(ByteBuffer rb);

    /**
     * called when channel is ready to read data,
     * reads data into the buffer and starts parsing and processing
     * of only fully received messages.
     * calls {@link PeerConnection#close(Peer.CloseReason)} ()} to drop the connection if read or parsing fails
     */
    void receive()
    {
        // due to method's agreement buffer here is always configured as:
        //  - .position: beginning of previously received data or zero if empty
        //  - .limit: end of the previously received data in buffer or zero
        ByteBuffer rb = recvBuffer;

        // configure buffer to append data as:
        // .position - start to write data from
        // .limit - allowed limit
        int position = rb.position();
        rb.position(rb.limit());

        if (!extendedReceiveBufferMode) {
            // in normal mode we use only part of the buffer
            // see {@link #receiveBufferNormalLimit}
            rb.limit(receiveBufferNormalLimit);
        } else {
            // in extended mode allow to receive only
            // tail of the last long message,
            // mode is switched off only after successful receive
            rb.limit(rb.position() + extendedReceiveBufferModeTailSize);
        }

        // read
        int n;
        try {
            n = channel.read(recvBuffer);
        } catch (IOException e) {
            // drop the connection
            if (DEBUG) System.out.println(peer.address + " error receiving data [3], " + e.getMessage());
            close(Peer.CloseReason.INACCESSIBLE);
            return;
        }

        if (n == -1) {
            // end of stream, close connection
            // that could be connection close in case of seed-seed
            if (DEBUG) System.out.println(peer.address + " closed the connection [4]");
            close(Peer.CloseReason.NORMAL);
            return;
        }

        if (n == 0) {
            // not data available
            return;
        }

        // track download speed
        rawStatistics.download.add(n);

        // revert buffer to default state to work with data
        int limit = rb.position();
        rb.limit(limit);
        rb.position(position);

        // this must leave .position at the beginning
        // of the unprocessed data
        int bytesToHaveFullMessage = processReceiveBuffer(rb);
        if (bytesToHaveFullMessage == -1) {
            close(Peer.CloseReason.PROTOCOL_ERROR);
            return;
        }
        // fix limit for possible errors in processing
        rb.limit(limit);

        if (rb.position() == rb.limit()) {
            // we have processed the whole buffer, no data left...
            // just reset the buffer to start from the beginning
            rb.position(0);
            rb.limit(0);
            extendedReceiveBufferMode = false;
        }
        else if (rb.limit() - rb.position() <= receiveBufferCompactionLimit) {
            // only small data in the buffer (not like half of PIECE message),
            // so compact the buffer
            rb.compact();
            rb.limit(rb.position());
            rb.position(0);
            extendedReceiveBufferMode = false; // just in case
        } else {
            // use upper part of the buffer to get the remaining part,
            // could be tested to find the optimum receiveBufferCompactionLimit
            // as system call vs copy
            extendedReceiveBufferMode = true;
            extendedReceiveBufferModeTailSize = bytesToHaveFullMessage;
        }
    }



    /**
     * serializes new messages to send buffer if there are any,
     * buffer state on method enter:
     *  .position   - position to place data/messages
     *  .limit      - limit of the space to populate
     *
     * @param sb buffer to populate with messages
     * @return true if there are more data to be sent, but not yet serialized into the buffer
     */
    abstract protected boolean populateSendBuffer(ByteBuffer sb);

    /**
     * low level sending operation, steps:
     * - calls func implementation to render new messages into the buffer
     * - sends prepared data from the send buffer to channel
     * - clears the buffer if everything is sent or compacts if not
     *
     * on io errors calls {@link PeerConnection#close(Peer.CloseReason)} ()} to drop the connection
     * and notify all parties
     *
     * buffer state:
     *  on enter:
     *    .position - index to data to be sent or 0 if empty
     *    .limit    - end of the rendered data or 0 if empty
     *  on exit: the same as on enter
     *
     * @return number of bytes sent
     */
    protected int send()
    {
        try {

            boolean hasMoreQueuedData = false;

            if (sendBufferNormalLimit <= sendBuffer.limit()) {
                // there is something large in the buffer above the normal  threshold,
                // that wasn't compacted on the previous turn, force sending without new data rendering
                if (DEBUG) System.out.println("send: normal limit exceeded: " + (sendBuffer.limit() - sendBufferNormalLimit));
            }
            else {
                // we are in normal mode but .position could be more than 0,
                // try to populate and proceed, buffer will be compacted eventually

                // prepare to append data
                int position = sendBuffer.position();
                sendBuffer.position(sendBuffer.limit());
                sendBuffer.limit(sendBuffer.capacity());

                // call implementation to render more data into the buffer
                hasMoreQueuedData = populateSendBuffer(sendBuffer);

                // revert to read all the rendered data
                sendBuffer.limit( sendBuffer.position());
                sendBuffer.position( position);
            }


            // send as much as possible & track raw upload speed
            int n = channel.write(sendBuffer);
            rawStatistics.upload.add(n);

            if (sendBuffer.position() == sendBuffer.limit()) {
                // all data has gone, reset buffer to the default state
                sendBuffer.position(0);
                sendBuffer.limit(0);
            }
            else if (sendBuffer.remaining() < sendBufferCompactionLimit) {
                // portion of data left, but it's small enough for compaction
                sendBuffer.compact();
            }

            // set up selector if we have more data to send
            if (hasMoreQueuedData || sendBuffer.hasRemaining()) {
                sKey.interestOpsOr(SelectionKey.OP_WRITE);
            } else {
                sKey.interestOpsAnd(~SelectionKey.OP_WRITE);
            }

            return n;
        } catch (IOException e) {
            if (DEBUG) System.out.println("send: " + peer.address + "  " + e.getMessage());
            close(Peer.CloseReason.INACCESSIBLE);
            return 0;
        }
    }

    /**
     * convenient method for implementation to indicate it has data to send
     */
    void setSendHasMoreData() {
        sendHasMoreData = true;
    }

    /**
     * facade method to simplifies calling side, calls IO operation in the channel
     * called when peer's channel is ready for IO operations
     */
    void onChannelReady()
    {
        if (sKey.isReadable()) {
            receive();
        }

        // key could become cancelled while handling receive,
        // connections could be dropped or io error raised
        if (!sKey.isValid()) {
            return;
        }

        if (sKey.isWritable()) {
            // we were interested in data, send it
            send();
        } else if (sendHasMoreData) {
            // seems we weren't interested, but some
            // data has appeared, wait for the next round
            sKey.interestOpsOr(SelectionKey.OP_WRITE);
            // reset flag as data pack will be processed ny send()
            sendHasMoreData = false;
        }
    }

}
