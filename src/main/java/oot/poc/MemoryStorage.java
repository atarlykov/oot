package oot.poc;

import oot.be.Metainfo;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Map;
import java.util.function.Consumer;

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
         * allowed constructor
         * @param _metainfo info about the torrent
         */
        public MemoryTorrentStorage(Metainfo _metainfo) {
            super(_metainfo, TorrentStorage.State.UNKNOWN);
            data = new byte[(int) metainfo.length];
        }

        @Override
        public void init(Consumer<Boolean> callback) {
            callback.accept(true);
        }

        @Override
        public void check(Consumer<Boolean> callback) {
            callback.accept(true);
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
            ByteBuffer wrap = ByteBuffer.wrap(data, (int) (index * metainfo.pieceLength + position), length);
            Block b = new Block(index, position, length, wrap);
            if (callback != null) {
                callback.accept(b);
            }
        }

        @Override
        public void release(Block block) {
        }

        @Override
        public void writeState(BitSet pieces, Map<Integer, Torrent.PieceBlocks> active) {
        }

        @Override
        public void readState(BitSet pieces, Map<Integer, Torrent.PieceBlocks> active, Consumer<Boolean> callback) {
            callback.accept(false);
        }
    }

    /**
     * allowed constructor
     * @param _blockSize size of blocks/buffers to use
     */
    public MemoryStorage(int _blockSize) {
        super(_blockSize);
    }

    @Override
    public TorrentStorage getStorage(Metainfo metainfo) {
        return new MemoryTorrentStorage(metainfo);
    }

    @Override
    public void write(String key, byte[] data) {

    }

    @Override
    public byte[] read(String key) {
        return new byte[0];
    }
}
