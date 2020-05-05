package oot;

import oot.be.Metainfo;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Storage engine that handles data operations for all torrents
 */
public abstract class Storage {


    /**
     * state of a torrent from the storage point of view
     */
    public enum TorrentStorageState {
        // usually this state is used on creation
        UNKNOWN,
        // process of space allocation is in process
        ALLOCATING,
        // torrent is being checked
        CHECKING,
        // ready for read/write
        READY,
        // some io error exists
        ERROR
    }

    /**
     * storage api for each torrent, created when torrent is added to client/storage.
     * stores torrent specific data and provides access to global storage api
     */
    public abstract class TorrentStorage {

        /**
         * ref to the metainfo of a torrent that is serviced with this instance
         */
        //Torrent torrent;
        Metainfo metainfo;

        /**
         * state of the this TorrentStorage
         */
        TorrentStorageState state;

        /**
         * @return generic state of the torrent storage
         */
        public TorrentStorageState getState() {
            return TorrentStorageState.UNKNOWN;
        }

        /**
         * initializes storage to be used for reading/writing,
         * could set READY state or leave it UNKNOWN, depends on the specific storage
         * @param callback callback to be notified on finish
         */
        public void init(Consumer<Boolean> callback) {}

        /**
         * starts check process
         * @param callback callback to be notified on finish
         */
        public void check(Consumer<Boolean> callback) {}

        /**
         * allocates new return cached instance of a buffer to be used
         * for block data
         * @return buffer
         */
        public abstract ByteBuffer getBuffer();

        /**
         * returns buffer to be reused later
         * @param buffer buffer to return
         */
        public abstract void releaseBuffer(ByteBuffer buffer);


        public void writeState(BitSet pieces, Map<Integer, Torrent.PieceBlocks> active) {}
        public void readState(BitSet pieces, Map<Integer, Torrent.PieceBlocks> active) {}


        /**
         * sends block of data to the storage for save operation, find correct place (file) that contains the block,
         * if block spans several files, it's written into all of them.
         * buffer's (position, limit) must match amount of data to be written (length).
         * this TorrentStorage must be in READY state.
         *
         * NOTE: operation could be performed asynchronously, with callback called from other thread.
         *
         * @param buffer buffer with data to read
         * @param index index of the piece
         * @param begin shift in the piece
         * @param length size of the data
         * @param callback callback to be notified on success/errors
         */
        public void write(ByteBuffer buffer, int index, int begin, int length, Consumer<Boolean> callback) {}

        /**
         * requests block of data to be read into the buffers from torrent's storage (file or some other place)
         * if block spans several files, reads from all of them.
         * buffer's (position, limit) must match amount of data to be read (length).
         * this TorrentStorage must be in READY state.
         *
         * NOTE: operation could be performed asynchronously, with callback called from other thread,
         * buffer could be allocated with call to {@link #getBuffer()} and (if yes) must be released
         * later with call to {@link #releaseBuffer(ByteBuffer)}
         *
         * @param buffer buffer to write data into
         * @param index index of the piece
         * @param begin shift in the piece
         * @param length size of the data
         * @param callback callback to be notified on success/errors
         */
        public void read(ByteBuffer buffer, int index, int begin, int length, Consumer<Boolean> callback) {}

        /**
         * this could be used to limit number of ingoing requests and/or send choke commands
         * @return number of blocks' read operations scheduled for execution
         */
        public int getReadQueueSize() {
            return 0;
        }

        /**
         * this could be used to limit number of ingoing requests and/or send choke commands
         * @return number of blocks' write operations scheduled for execution
         */
        public int getWriteQueueSize() {
            return 0;
        }
    }


    public abstract void stop();

    /**
     * create instance of {@link TorrentStorage} to be used from corresponding {@link Torrent} class
     * @param metainfo metainfo of the torrent
     * @return instance to be used as api to storage
     */
    public abstract TorrentStorage getStorage(Metainfo metainfo);

}
