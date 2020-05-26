package oot;

import oot.be.Metainfo;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Storage engine that handles data operations for all torrents
 */
public abstract class Storage {

    int blockSize;


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
        Metainfo metainfo;
        /**
         * state of the this TorrentStorage
         */
        TorrentStorageState state;

        /**
         * allowed constructor
         * @param metainfo meta info of the torrent
         * @param state initial state
         */
        public TorrentStorage(Metainfo metainfo, TorrentStorageState state) {
            this.metainfo = metainfo;
            this.state = state;
        }

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
         * todo: must find correct pieces
         * starts check process
         * @param callback callback to be notified on finish
         */
        public void check(Consumer<Boolean> callback) {}

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
        public void writeBlock(ByteBuffer buffer, int index, int begin, int length, Consumer<Boolean> callback) {}

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
        public void readBlock(ByteBuffer buffer, int index, int begin, int length, Consumer<Boolean> callback) {}

        /**
         * writes state of the torrent to be available later in case of restarts
         * @param pieces state of pieces
         * @param active state of blocks for pieces being downloaded
         */
        public void writeState(BitSet pieces, Map<Integer, Torrent.PieceBlocks> active) {}

        /**
         * reads state of the torrent into the specified structures
         * @param pieces will be populated with pieces state
         * @param active will be populated with active pieces state
         * @param callback callback to be notified with true if state was successfully restored and
         *                 false if it's missing or there were some errors
         */
        public void readState(BitSet pieces, Map<Integer, Torrent.PieceBlocks> active, Consumer<Boolean> callback) {}

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

        /**
         * allocates new or returns cached instance of a buffer to be used
         * for reading/writing block data
         * @return buffer
         */
        public ByteBuffer getBuffer() {
            return ByteBuffer.allocateDirect(blockSize);
        }

        /**
         * returns buffer to be reused later
         * @param buffer buffer to return
         */
        public void releaseBuffer(ByteBuffer buffer) {}
    }


    /**
     * called to perform initialization
     */
    public void init() {}

    /**
     * called to stop the storage
     */
    public void stop() {}

    /**
     * writes some data under the specified key,
     * could be used to save DHT table or something else
     * @param key key
     * @param data data to save
     */
    public abstract void write(String key, byte[] data);

    /**
     * reads previously saved data under the specified key
     * @param key key
     * @return data if found or null
     */
    public abstract byte[] read(String key);

    /**
     * allowed constructor
     * @param blockSize size of the block we are operating with,
     *                  used for buffers allocations
     */
    public Storage(int blockSize) {
        this.blockSize = blockSize;
    }

    /**
     * create instance of {@link TorrentStorage} to be used from corresponding {@link Torrent} class
     * as a facade to storage engine
     * @param metainfo metainfo of the torrent
     * @return instance to be used as api to storage
     */
    public abstract TorrentStorage getStorage(Metainfo metainfo);

}
