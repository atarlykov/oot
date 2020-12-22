package oot.poc;

import oot.be.Metainfo;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Map;
import java.util.function.Consumer;

/**
 * storage api for each torrent, created when torrent is added to client/storage.
 * stores torrent specific data and provides access to global storage api
 */
public abstract class TorrentStorage {

    /**
     * size of a block of peer protocol, usually is the constant 16K,
     * todo: review and move/? with storage
     */
    int blockSize;

    /**
     * state of a torrent from the storage point of view
     */
    public enum State {
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
     * describes block of data to be read or written,
     * used to collect data during async calls
     */
    public class Block {
        // piece index
        public int index;
        // block position/begin
        public int position;
        // block length
        public int length;
        // ref to buffer with data
        public ByteBuffer data;
        // optional param
        public Object param;

        public Block(int index, int position, int length, ByteBuffer data, Object param) {
            this.index = index;
            this.position = position;
            this.length = length;
            this.data = data;
            this.param = param;
        }

        public Block(int index, int position, int length, ByteBuffer data) {
            this.index = index;
            this.position = position;
            this.length = length;
            this.data = data;
        }

        public Block(int index, int position, int length) {
            this.index = index;
            this.position = position;
            this.length = length;
        }

        public void release() {
            TorrentStorage.this.release(this);
        }
    }

    /**
     * ref to the metainfo of a torrent that is serviced with this instance
     */
    Metainfo metainfo;
    /**
     * state of the this TorrentStorage
     */
    State state;

    /**
     * allowed constructor
     *
     * @param metainfo meta info of the torrent
     * @param state    initial state
     */
    public TorrentStorage(Metainfo metainfo, State state) {
        this.metainfo = metainfo;
        this.state = state;
    }

    /**
     * @return generic state of the torrent storage
     */
    public State getState() {
        return State.UNKNOWN;
    }

    /**
     * initializes storage to be used for reading/writing,
     * could set READY state or leave it UNKNOWN, depends on a specific storage
     *
     * @param callback callback to be notified on finish
     */
    public abstract void init(Consumer<Boolean> callback);

    /**
     * todo: must find correct pieces
     * starts check process
     *
     * @param callback callback to be notified on finish todo: change
     */
    public abstract void check(Consumer<Boolean> callback);

    /**
     * MUST be called by a client code after any block is sent to it
     * via callback in {@link #read(int, int, int, Object, Consumer)} method.
     * This releases buffer for reuse... MUST be called, possible to go with WeakRef, but ...
     *
     * @param block block to release/unlock
     */
    public abstract void release(Block block);

    /**
     * sends block of data to the storage for save operation, find correct place (file) that contains the block,
     * if block spans several files, it's written into all of them.
     * buffer's (position, limit) must match amount of data to be written (length).
     * this TorrentStorage must be in READY state.
     * <p>
     * NOTE: operation could be performed asynchronously, with callback called from other thread.
     * NOTE: buffer MUST NOT be referenced in any async implementation after exit from this method
     *
     * @param buffer   buffer with data to read
     * @param index    index of the piece
     * @param position shift in the piece
     * @param length   size of the data
     * @param callback callback to be notified on success/errors
     */
    public abstract void write(ByteBuffer buffer, int index, int position, int length, Consumer<Block> callback);

    /**
     * requests block of data to be read into the buffers from torrent's storage (file or some other place)
     * if block spans several files, reads from all of them.
     * buffer's (position, limit) must match amount of data to be read (length).
     * this TorrentStorage must be in READY state.
     * <p>
     * NOTE: operation could be performed asynchronously, with callback called from other thread,
     * buffer MUST be releases with a call to {@link #release(Block)}
     * <p>
     * block can't be referenced after block.release
     *
     * @param index    index of the piece
     * @param position shift in the piece
     * @param length   size of the data
     * @param param    optional param to be returned as block.param via callback (usually this is a ref to pc)
     * @param callback callback to be notified on success/errors
     */
    public abstract void read(int index, int position, int length, Object param, Consumer<Block> callback);

    /**
     * writes state of the torrent to be available later in case of restarts
     * @param pieces state of pieces
     * @param active state of blocks for pieces being downloaded
     */
    public abstract void writeState(BitSet pieces, Map<Integer, Torrent.PieceBlocks> active);

    /**
     * reads state of the torrent into the specified structures
     * @param pieces will be populated with pieces state
     * @param active will be populated with active pieces state
     * @param callback callback to be notified with true if state was successfully restored and
     *                 false if it's missing or there were some errors
     */
    public abstract void readState(BitSet pieces, Map<Integer, Torrent.PieceBlocks> active, Consumer<Boolean> callback);

    /**
     * this could be used to limit number of ingoing requests and/or send choke commands
     *
     * @return number of blocks' read operations scheduled for execution
     */
    public int getReadQueueSize() {
        return 0;
    }

    /**
     * this could be used to limit number of ingoing requests and/or send choke commands
     *
     * @return number of blocks' write operations scheduled for execution
     */
    public int getWriteQueueSize() {
        return 0;
    }

}
