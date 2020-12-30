package oot.storage;

import oot.Torrent;
import oot.be.Metainfo;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Map;
import java.util.function.BiConsumer;
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
        UNKNOWN,        // usually this state is used on creation
        BINDING,        // checking if resources (files) are available and correct (size)
        ALLOCATE,       // process of space allocation is in progress (missing parts)
        CHECK,          // torrent is being checked piece by piece and/or creates/allocates missing part
        BOUND,          // resource are bound, ready for read/write
        ERROR           // some io error has occurred

        /**
         * based on Torrent.pieces state,
         *
         *
         *
         *                                        +----(sizes ok)-----------------------+
         *                                        |                                     |
         *                                        |                                    \|/
         * UNKNOWN  --( torrent state ) ---> BINDING --(no files)--> ALLOCATING ----> BOUND <-+----------------+
         *                                        |                                     |                      |
         *                                        |                                    \|/                     |
         *                                        +----(missing/sizes error)--------> ERROR --+-> CHECK/ALLOC -+
         *                                                                             /|\        (tor state)
         *                                                                              |
         *                                                              on io error ----+
         *  Could be in stationary states: BOUND, ERROR
         *  other states are intermediate, client should wait.
         *  Maybe ERROR --> ALLOCATING  --> BOUND where ALLOCATING is allocation and/or just files creation
         *
         *  BINDING / CHECk/ALLOC
         *
         *
         *                                    +------(allocate?, )----------------------------+
         *                                    |                                               |
         *                                    |   +----(sizes ok)-----------------------+     |
         *                                    |   |                                     |     |
         *                                   \|/  |                                    \|/    |
         * UNKNOWN  --( torrent state ) ---> BINDING --(no files)--> ALLOCATING ----> BOUND   |
         *                                        |                                     |     |
         *                                        |                                     |     |
         *                  (missing/sizes error) +----- (recheck) ---- CHECK ----------+     |
         *                   need pieces recheck  |         true       +alloc                 |
         *                                        |                                           |
         *                                        +---------------------------------> ERROR --+
         *                                                                             /|\
         *                                                                              |
         *                                                              on io error ----+
         *
         *
         *                                                          ERROR_PIECES    ERROR
         *                                                          ERROR(piecesError, io, ..)
         *
         *
         */
    }

    /**
     * type of error when storage is in ERROR state
     */
    public enum ErrorType {
        INTEGRITY,      // torrent needs pieces' check (hashes)
        IO              // some io error occurred (message)
    }

    /**
     * details of error when storage is in ERROR state
     */
    public static class ErrorDetails
    {
        public final ErrorType type;
        public final String message;

        public ErrorDetails(ErrorType type, String message) {
            this.type = type;
            this.message = message;
        }
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
        public ByteBuffer buffer;
        // optional param
        public Object param;

        public Block(int index, int position, int length, ByteBuffer buffer, Object param) {
            this.index = index;
            this.position = position;
            this.length = length;
            this.buffer = buffer;
            this.param = param;
        }

        public Block(int index, int position, int length, ByteBuffer buffer) {
            this.index = index;
            this.position = position;
            this.length = length;
            this.buffer = buffer;
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
     * ref to the meta info of a torrent that is serviced with this instance
     */
    Metainfo metainfo;
    /**
     * state of this TorrentStorage
     */
    volatile State state;
    /**
     * optional error details if state == {@link State#ERROR}
     */
    volatile ErrorDetails error;

    /**
     * allowed constructor
     *
     * @param metainfo meta info of the torrent
     */
    public TorrentStorage(Metainfo metainfo) {
        this.metainfo = metainfo;
        this.state = State.UNKNOWN;
    }

    /**
     * @return generic state of the torrent storage
     */
    public State getState() {
        return state;
    }

    /**
     * @return error details of present, could be null,
     * getState() == ERROR doesn't guarantee (getErrorStateDetails() != null) as the next step
     */
    public ErrorDetails getErrorStateDetails() {
        return error;
    }

    /**
     * tries to move torrent into State.BOUND from any current state,
     * call can be ignored if this storage is in some transition state, like:
     *  UNKNOWN ---> BINDING ---> ... BOUND | ERROR
     *  ERROR   ---> BINDING(allocate, piecesCheck) ---> ... BOUND | ERROR
     *  BOUND   ---> ignored (callback will be called)
     *  others  ---> ignored (callback could be not called)
     *
     * @param newTorrent torrent is new and existing files are not allowed (todo: review)
     * @param allocate space may be allocated during subsequent ALLOCATE phase if true
     * @param recheck run pieces recheck phase if true
     * @param callback optional callback to be notified on finish with new state and (optionally) with
     *                 result of pieces check
     */
    public abstract void bind(
            boolean newTorrent,
            boolean allocate, boolean recheck,
            BiConsumer<State, BitSet> callback);

    /**
     * starts whole torrent check process, calculates and
     * validates hashes of all the pieces against meta info data,
     * notifies callback with a result, bitset will contain "set" bit
     * in place of each correct piece
     *
     * @param callback callback to be notified on finish
     */
    public abstract void check(Consumer<BitSet> callback);

    /**
     * calculates hash of the specified piece and validates
     * it against meta info
     * @param piece piece index
     * @param callback callback, true in case of piece is correct
     */
    public abstract void check(int piece, Consumer<Boolean> callback);

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
     * Requests block of data to be read from torrent's storage (file or some other place)
     * into a new buffer (allocated or cached) inside Block returned.
     * Returned buffer's position and limit ARE point to the beginning and the end of the data read.

     * NOTE: This TorrentStorage must be in READY state.
     * NOTE: operation could be performed asynchronously, with callback called from other thread,
     * NOTE: buffer MUST be releases with a call to {@link #release(Block)}
     * NOTE: block can't be referenced after block.release
     *
     * @param index    index of the piece
     * @param position shift in the piece
     * @param length   size of the data
     * @param param    optional param to be returned as block.param via callback (usually this is a ref to pc)
     * @param callback callback to be notified on success/errors
     */
    public abstract void read(int index, int position, int length, Object param, Consumer<Block> callback);

    /**
     * MUST be called by a client code after any block is sent to it
     * via callback in {@link #read(int, int, int, Object, Consumer)} method.
     * This releases buffer for reuse... MUST be called, possible to go with WeakRef, but ...
     *
     * @param block block to release/unlock
     */
    public abstract void release(Block block);

    /**
     * writes state of the torrent to be available later in case of restarts,
     * it must be safe to store references inside the implementation, calling
     * party must send copies here
     *
     * @param pieces state of pieces
     * @param active state of blocks for pieces being downloaded
     */
    public abstract void writeState(BitSet pieces, Map<Integer, Torrent.PieceBlocks> active);

    /**
     * reads state of the torrent into the specified structures
     *
     * @param pieces will be populated with pieces state
     * @param active will be populated with active pieces state
     * @param callback callback to be notified with true if state was successfully restored and
     *                 false if it's missing or there were some errors
     */
    public abstract void readState(
            BitSet pieces,
            /*todo: remove?*/Map<Integer, Torrent.PieceBlocks> active,
            Consumer<Boolean> callback);

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
