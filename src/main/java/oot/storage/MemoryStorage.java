package oot.storage;

import oot.Torrent;
import oot.be.Metainfo;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Storage that allocates byte array for each torrent (size is limited as int max).
 * Writes and reads data synchronously inside a calling thread.
 * Could be used for testing or downloading one-file torrents.
 */
public class MemoryStorage extends Storage
{
    private final static boolean DEBUG = true;

    /**
     * instances of this class will be created for each torrent inside a client,
     * to perform storing and reads of data
     */
    public static class MemoryTorrentStorage extends TorrentStorage {
        /**
         * torrent data
         */
        final byte[] data;
        /**
         * digest to be used for pieces validation,
         * assume that all calls in bounds of one torrent are single threaded
         */
        protected MessageDigest digest;
        /**
         * stores torrent state if called to write it
         */
        protected BitSet pieces;
        /**
         * stores torrent state if called to write it
         */
        protected Map<Integer, Torrent.PieceBlocks> blocks;

        /**
         * allowed constructor
         * @param _metainfo info about the torrent
         */
        public MemoryTorrentStorage(Metainfo _metainfo)
        {
            super(_metainfo);
            data = new byte[(int) metainfo.length];
            try {
                digest = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("SHA-1 is not available", e);
            }
        }

        @Override
        public void bind(boolean newTorrent, boolean allocate, boolean recheck, BiConsumer<State, BitSet> callback)
        {
            // just switch to the ready state
            state = State.BOUND;

            if (callback != null) {
                if (recheck) {
                    check(status -> callback.accept(State.BOUND, status));
                } else {
                    callback.accept(State.BOUND, null);
                }
            }
        }

        @Override
        public void check(Consumer<BitSet> callback)
        {
            BitSet mask = new BitSet((int) metainfo.pieces);
            for (int i = 0; i < metainfo.pieces; i++) {
                boolean correct = check(i);
                mask.set(i, correct);
            }
            callback.accept(mask);
        }

        @Override
        public void check(int piece, Consumer<Boolean> callback)
        {
            boolean correct = check(piece);
            callback.accept(correct);
        }

        /**
         * checks piece in data array to be correct
         * @param piece piece index
         * @return true if correct
         */
        protected boolean check(int piece)
        {
            digest.reset();
            int base = (int) metainfo.pieceLength * piece;
            int length = (int) metainfo.pieceLength(piece);
            digest.update(data, base, length);

            return metainfo.checkPieceHash(piece, digest.digest());
        }

        @Override
        public void write(ByteBuffer buffer, int index, int position, int length, Consumer<Block> callback)
        {
            buffer.get(data, (int)(index * metainfo.pieceLength + position), length);
            if (callback != null) {
                Block b = new Block(index, position, length);
                callback.accept(b);
            }
        }

        @Override
        public void read(int index, int position, int length, Object param, Consumer<Block> callback)
        {
            if (callback != null) {
                ByteBuffer wrapper = ByteBuffer.wrap(data, (int) (index * metainfo.pieceLength + position), length);
                Block b = new Block(index, position, length, wrapper);
                callback.accept(b);
            }
        }

        @Override
        public void release(Block block) {
            // no blocks are used/locked
        }

        @Override
        public void writeState(BitSet _pieces, Map<Integer, Torrent.PieceBlocks> _active)
        {
            // torrent sends us a copy
            pieces = _pieces;
            blocks = _active;
        }

        @Override
        public void readState(BitSet _pieces, Map<Integer, Torrent.PieceBlocks> _active, Consumer<Boolean> _callback)
        {
            if ((pieces == null) || (blocks == null)) {
                if (_callback != null) {
                    _callback.accept(true);
                    return;
                }
            }
            if (_pieces != null) {
                _pieces.clear();
                _pieces.or(pieces);
            }
            if (_active != null) {
                _active.clear();
                _active.putAll(blocks);
            }
        }
    }

    /**
     * configuration storage if used
     */
    Map<String, byte[]> configuration;

    /**
     * allowed constructor
     * @param _blockSize size of blocks/buffers to use
     */
    public MemoryStorage(int _blockSize) {
        super(_blockSize);
    }

    @Override
    public TorrentStorage getStorage(Metainfo metainfo)
    {
        return new MemoryTorrentStorage(metainfo);
    }

    @Override
    public void write(String key, byte[] data)
    {
        if (configuration == null) {
            configuration = new HashMap<>();
        }
        configuration.put(key, data);
    }

    @Override
    public byte[] read(String key) {
        if (configuration == null) {
            return null;
        }
        return configuration.get(key);
    }
}
