package oot.storage;

import oot.Torrent;
import oot.be.Metainfo;


import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Storage that allows writing torrent data to disk and reading
 * it back synchronously and without any caching, uses common root folder
 * for all active torrents
 *
 * that usually works fine, but blocks network operations,
 * the same time this prevent client from flooding with requests
 *
 * supports only one fixed block size (could be extended, needs refactoring)
 *
 * uses {@link java.util.concurrent.ExecutorService} to run long tasks
 * like disk space allocations and torrent checks
 */
public class SimpleFileStorage extends Storage
{
    private final static boolean DEBUG = true;

    /**
     * simple internal container to link file information from metainfo
     * to specific file channel to read/write data
     */
    protected static class TorrentFile {
        FileChannel channel;
        Metainfo.FileInfo info;

        public TorrentFile(Metainfo.FileInfo info) {
            this.info = info;
        }
    }

    /**
     * instances of this class will be created for each torrent inside a client,
     * to perform storing and reads of data
     */
    public class SimpleFileTorrentStorage extends TorrentStorage {
        /**
         * files of a torrent, based on metadata
         */
        final TorrentFile[] files;
        /**
         * cumulative positions of files' ends inside global torrent data
         */
        final long[] filesEndSizeSums;
        /**
         * digest to be used for pieces validation,
         * assume that all calls in bounds of one torrent are single threaded
         */
        protected MessageDigest digest;

        /**
         * allowed constructor
         * @param _metainfo info about the torrent
         */
        public SimpleFileTorrentStorage(Metainfo _metainfo)
        {
            super(_metainfo);
            files = new TorrentFile[metainfo.files.size()];
            filesEndSizeSums = new long[metainfo.files.size()];

            // calculate "end sums" to quickly map pieces to files during io
            long sum = 0L;
            for (int i = 0; i < metainfo.files.size(); i++) {
                sum += metainfo.files.get(i).length;
                filesEndSizeSums[i] = sum;
            }

            try {
                digest = MessageDigest.getInstance("SHA-1");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("SHA-1 is not available", e);
            }
        }

        @Override
        public void bind(boolean newTorrent, boolean allocate, boolean recheck, BiConsumer<State, BitSet> callback) {
            SimpleFileStorage.this._bind(this, newTorrent, allocate, recheck, callback);
        }


        @Override
        public void check(Consumer<BitSet> callback)
        {
            if (callback != null)
            {
                BitSet mask = new BitSet((int) metainfo.pieces);
                for (int i = 0; i < metainfo.pieces; i++) {
                    boolean correct = check(i);
                    mask.set(i, correct);
                }
                callback.accept(mask);
            }
        }

        @Override
        public void check(int piece, Consumer<Boolean> callback)
        {
            if (callback != null) {
                boolean correct = check(piece);
                callback.accept(correct);
            }
        }

        /**
         * checks piece in data array to be correct
         * @param piece piece index
         * @return true if correct
         */
        protected boolean check(int piece)
        {
            blockSize = 16384;

            ByteBuffer buffer = getBuffer(blockSize);
            digest.reset();

            int length = (int) metainfo.pieceLength(piece);
            int blocks = length / blockSize;

            int position = 0;
            for (int i = 0; i < blocks; i++, position += blockSize)
            {
                buffer.clear();
                boolean ok = _read(this, buffer, piece, position, blockSize);
                if (!ok) {
                    return false;
                }
                digest.update(buffer);
            }

            // check and process tail in case of last piece
            int tail = length % blockSize;
            if (0 < tail) {
                buffer.clear();
                boolean ok = _read(this, buffer, piece, position, tail);
                if (!ok) {
                    return false;
                }
                digest.update(buffer);
            }

            releaseBuffer(buffer);

            return metainfo.checkPieceHash(piece, digest.digest());
        }

        @Override
        public void write(ByteBuffer buffer, int index, int position, int length, Consumer<Block> callback)
        {
            // todo:  just use the buffer here
            ByteBuffer tmp = getBuffer(length);
            tmp.put(buffer).flip();

            boolean result = _save(this, tmp, index, position, length);
            releaseBuffer(tmp);
            if (callback != null) {
                Block b = new Block(index, position, length);
                b.index = index;
                b.position = position;
                b.length = length;
                callback.accept(b);
            }
        }

        @Override
        public void read(int index, int position, int length, Object param, Consumer<Block> callback)
        {
            Block b = new Block(index, position, length, getBuffer(length), param);
            boolean result = _read(this, b.buffer, index, position, length);

            System.out.println(" [sfs]  read: " + index + "," + position + "," + length +
                    "  [0]:" + b.buffer.get(0) + "   " + b.buffer + "   result:" + result);

            if (callback != null) {
                callback.accept(b);
            }
        }

        @Override
        public void release(Block block) {
            releaseBuffer(block.buffer);
        }

        @Override
        public void writeState(BitSet pieces, Map<Integer, Torrent.PieceBlocks> active) {
            SimpleFileStorage.this.writeState(metainfo, pieces, active);
        }

        @Override
        public void readState(BitSet pieces, Map<Integer, Torrent.PieceBlocks> active, Consumer<Boolean> callback) {
            SimpleFileStorage.this.readState(metainfo, pieces, active, callback);
        }
    }

    /**
     * common root folder to be used as default root path
     * for all torrents and other data
     */
    protected Path root;

    /**
     * do we need to preallocate space by default
     */
    protected boolean preallocate = true;

    /**
     * cached buffers
     */
    protected final ArrayDeque<ByteBuffer> cache = new ArrayDeque<>();

    // debug
    protected long buffersAllocated = 0;

    /**
     * executor for long operations line pre-allocation and checking
     */
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);


    /**
     * allowed constructor
     * @param _root common root folder for storing torrents' data
     * @param _preallocate files will be pre-allocated if true
     * @param _blockSize size of blocks/buffers to use
     */
    public SimpleFileStorage(Path _root, boolean _preallocate, int _blockSize) {
        super(_blockSize);
        this.root = _root;
        this.preallocate = _preallocate;
        this.blockSize = _blockSize;
    }

    @Override
    public TorrentStorage getStorage(Metainfo metainfo) {
        return new SimpleFileTorrentStorage(metainfo);
    }

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void stop() {
        executor.shutdown();
    }


    /**
     * allocates new or allows to reuse a buffer for io operation,
     * buffer could be large than requested (usually equals to block size),
     * position will be set to zero and limit to the length specified
     *
     * @param length size of the buffer needed in bytes, usually equals to block size of torrents,
     *               but could be smaller in case if some data is already available on the other side
     *               (that is real situation in the wild)
     * @return cached or new allocated buffer
     */
    ByteBuffer getBuffer(int length)
    {
        assert length <= blockSize : "too large buffer requested";
        ByteBuffer buffer;
        synchronized (cache) {
            buffer = cache.pollFirst();
            if (buffer == null) {
                buffersAllocated++;
                buffer = ByteBuffer.allocateDirect(blockSize);
                //System.out.println(" [sfs]  getBuffer: allocated:" + System.identityHashCode(buffer) + "     " + buffersAllocated);
            } else {
                //System.out.println(" [sfs]  getBuffer: reused:" + System.identityHashCode(buffer) + "     " + buffersAllocated);
            }
        }
        // prepare buffer to read the data
        buffer.position(0).limit(length);
        return buffer;
    }

    /**
     * returns buffer to cache for reuse
     * @param buffer buffer
     */
    void releaseBuffer(ByteBuffer buffer) {
        //System.out.println(" [sfs]  releaseBuffer:" + System.identityHashCode(buffer));
        synchronized (cache) {
            buffer.clear();
            cache.offerFirst(buffer);
        }
    }

    class BindStatus {
        int missing;
        int exist;
        int incorrectSize;
    }


    /**
     * initializes torrent specific storage to be used for reading/writing,
     * tries to move torrent into State.BOUND from any current state,
     * call can be ignored if this storage is in some transition state, like:
     *  UNKNOWN ---> BINDING ---> ... BOUND | ERROR
     *  ERROR   ---> BINDING(allocate, piecesCheck) ---> ... BOUND | ERROR
     *  BOUND   ---> ignored (callback will be called)
     *  others  ---> ignored (callback could be not called)
     *
     * @param ts specific torrent
     * @param newTorrent if torrent is new than check could be run if files exist
     * @param allocate space may be allocated during subsequent ALLOCATE phase if true
     * @param recheck run pieces recheck phase if true
     * @param callback optional callback to be notified on finish
     */
    protected void _bind(
            SimpleFileTorrentStorage ts,
            boolean newTorrent, boolean allocate, boolean recheck,
            BiConsumer<TorrentStorage.State, BitSet> callback)
    {
        TorrentStorage.State tsState = ts.state;
        if (tsState == TorrentStorage.State.BOUND) {
            // may run recheck if requested
            if (callback != null) callback.accept(TorrentStorage.State.BOUND, null);
            return;
        }

        // collect files information
        BindStatus bStatus = new BindStatus();

        if (tsState == TorrentStorage.State.UNKNOWN) {
            try {
                ts.state = TorrentStorage.State.BINDING;
                _bind(bStatus, ts.metainfo, ts.files);

                if (bStatus.incorrectSize != 0) {
                    // files already exist and have incorrect size
                    ts.state = TorrentStorage.State.ERROR;
                    ts.error = new TorrentStorage.ErrorDetails(TorrentStorage.ErrorType.INTEGRITY, "incorrect files exist");
                    if (callback != null) callback.accept(TorrentStorage.State.ERROR, null);
                    return;
                }

                if (newTorrent && (bStatus.exist != 0)) {
                    // new torrent, do files must be checked
                    ts.state = TorrentStorage.State.ERROR;
                    ts.error = new TorrentStorage.ErrorDetails(TorrentStorage.ErrorType.INTEGRITY, "unknown files exist");
                    if (callback != null) callback.accept(TorrentStorage.State.ERROR, null);
                    return;
                }

                if (allocate) {
                    ts.state = TorrentStorage.State.ALLOCATE;
                    _allocate(ts);
                }
                ts.state = TorrentStorage.State.BOUND;
                if (callback != null) callback.accept(TorrentStorage.State.BOUND, null);
            }
            catch (IOException e) {
                ts.state = TorrentStorage.State.ERROR;
                ts.error = new TorrentStorage.ErrorDetails(TorrentStorage.ErrorType.IO, e.getMessage());
                if (callback != null) callback.accept(TorrentStorage.State.ERROR, null);
            }
        }

        if (tsState == TorrentStorage.State.ERROR) {
            try {
                ts.state = TorrentStorage.State.BINDING;
                _bind(bStatus, ts.metainfo, ts.files);
                // that could be ok to have errors in ERROR state,

                // try to allocate if requested
                if (allocate) {
                    ts.state = TorrentStorage.State.ALLOCATE;
                    _allocate(ts);
                }

                // force pieces check if requested
                BitSet mask = null;
                if (recheck) {
                    ts.state = TorrentStorage.State.CHECK;
                    mask = new BitSet((int) ts.metainfo.pieces);
                    for (int i = 0; i < ts.metainfo.pieces; i++) {
                        boolean correct = ts.check(i);
                        mask.set(i, correct);
                    }
                }

                ts.state = TorrentStorage.State.BOUND;
                if (callback != null) callback.accept(TorrentStorage.State.BOUND, mask);
            } catch (IOException e) {
                ts.state = TorrentStorage.State.ERROR;
                ts.error = new TorrentStorage.ErrorDetails(TorrentStorage.ErrorType.IO, e.getMessage());
                if (callback != null) callback.accept(TorrentStorage.State.ERROR, null);
            }
        }
    }


    /**
     * creates directory structure for multi file torrents,
     * creates files on disk for all torrent's files and
     * bind newly created file channel to them.
     * @param bStatus status to populate during bind
     * @param metainfo torrent meta info
     * @param files containers to hold binding info
     * @return number of files that were bound, but seem to have incorrect parameters
     * @throws IOException if any
     */
    protected void _bind(BindStatus bStatus, Metainfo metainfo, TorrentFile[] files) throws IOException
    {
        if (metainfo.files.size() != files.length) {
            throw new IOException("incorrect torrent configuration (dev error)");
        }

        if (metainfo.isMultiFile())
        {
            // check if root torrent directory exists
            Path path = root.resolve(metainfo.directory);
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }

            // handle all torrent files
            int index = 0;
            for (Metainfo.FileInfo fileInfo: metainfo.files)
            {
                assert 0 < fileInfo.names.size() : "incorrect file info (meta info must be validated on creation)";

                // close if we are re-binding
                if ((files[index] != null) && (files[index].channel != null)) {
                    try {
                        files[index].channel.close();
                    } catch (IOException e) {
                    }
                }

                TorrentFile tFile = new TorrentFile(fileInfo);
                files[index++] = tFile;

                // check and create intermediate directories
                for (int i = 0; i < fileInfo.names.size() - 1; i++) {
                    path = path.resolve(fileInfo.names.get(i));
                    if (!Files.exists(path)) {
                        Files.createDirectories(path);
                    }
                }

                // get file name without directories
                path = path.resolve(fileInfo.names.get(fileInfo.names.size() - 1));
                _bind(bStatus, path, fileInfo, tFile);
            }
        }
        else {
            assert 0 < metainfo.files.size() : "incorrect file info (meta info must be validated on creation)";

            Metainfo.FileInfo fileInfo = metainfo.files.get(0);
            assert 0 < fileInfo.names.size() : "incorrect file info (meta info must be validated on creation)";

            String[] names = fileInfo.names.toArray(String[]::new);
            Path path = Paths.get(root.toString(), names);

            // close if we are re-binding
            if ((files[0] != null) && (files[0].channel != null)) {
                try {
                    files[0].channel.close();
                } catch (IOException e) {
                }
            }

            files[0] = new TorrentFile(fileInfo);
            _bind(bStatus, path, fileInfo, files[0]);
        }
    }

    /**
     * initializes file channel for the given file path and populates it into the torrent file,
     * checks if file already exists and has incorrect size
     *
     * @param bStatus status to populate
     * @param path full path the file we must open
     * @param fileInfo file info from the torrent
     * @param tFile torrent file to populate
     * @return true if there are file inconsistencies
     * @throws IOException if case of any error
     */
    protected void _bind(BindStatus bStatus, Path path, Metainfo.FileInfo fileInfo, TorrentFile tFile) throws IOException
    {
        File file = path.toFile();

        if (!file.exists())
        {
            // create new file and open it
            tFile.channel = FileChannel.open(path,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            // no errors
            bStatus.missing++;
            return;
        }
        else {
            tFile.channel = FileChannel.open(path,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);

            if (file.length() == fileInfo.length) {
                // already exists, that's ok
                bStatus.exist++;
                return;
            }
            else if (file.length() < fileInfo.length) {
                // that's ok, could be allocated later
                bStatus.exist++;
                return;
            }
            else {
                // that's strange, unknown file?
                bStatus.exist++;
                bStatus.incorrectSize++;
                return;
            }
        }
    }

    /**
     * allocates space for all files
     * @throws IOException if any
     */
    protected void _allocate(SimpleFileTorrentStorage ts) throws IOException {
        for (int i = 0; i < ts.files.length; i++) {
            TorrentFile file = ts.files[i];
            if (file.channel.size() < file.info.length) {
                file.channel.write(ByteBuffer.wrap(new byte[] {0}), file.info.length - 1);
                file.channel.force(false);
            }
        }
    }

    /**
     * stores block of data into the file that contains the block,
     * if block spans several files, it's written into all of them.
     * buffer's (position, limit) must match amount of data to be written (length)
     * @param buffer buffer with data to write
     * @param index index of the piece
     * @param begin shift in the piece
     * @param length size of the data
     * @return true on success and false if position is incorrect or io error
     */
    protected boolean _save(SimpleFileTorrentStorage ts, ByteBuffer buffer, int index, int begin, int length)
    {
        // linear address of the 1st byte to write
        long la = ts.metainfo.pieceLength * index + begin;

        // find index of the file where the 1st byte is located
        int file = 0;
        while (ts.filesEndSizeSums[file] <= la) {
            file++;
            if (file == ts.filesEndSizeSums.length) {
                return false;
            }
        }

        try {
            // support write into several files
            // if the piece overlaps them all
            do {
                // number of bytes available in file after 'begin' position
                long bytesLeftInFile = ts.filesEndSizeSums[file] - la;
                // position in file to start writing
                long positionInFile = ts.files[file].info.length - bytesLeftInFile;
                // file could be too short, write only allowed number of bytes
                long toWrite = Math.min(length, bytesLeftInFile);

                // buffer has (position, limit) that points to the data left
                int limit = buffer.limit();
                buffer.limit(buffer.position() + (int)toWrite);

                ts.files[file].channel.write(buffer, positionInFile);

                // restore limit
                buffer.limit(limit);

                // switch to the next file
                file++;
                // correct linear address to write to and bytes left
                la += toWrite;
                length -= toWrite;
            } while (0 < length);
        } catch (IOException ignored) {
            return false;
        }

        return true;
    }

    /**
     * reads block of data from the necessary files of the torrent in the specified buffer,
     * buffer's (position, limit) must match amount of data to be written (length)
     * @param buffer buffer to read into
     * @param index index of the piece
     * @param begin shift in the piece
     * @param length size of the data
     * @return true on success and false if position is incorrect or io error
     */
    private boolean _read(SimpleFileTorrentStorage ts, ByteBuffer buffer, int index, int begin, int length)
    {
        //System.out.println(" [sfs]  _read enter: " + index + "," + begin + "," + length + "   buf:" + buffer.position() + "," + buffer.limit());
        // linear address of the 1st byte to read
        long la = ts.metainfo.pieceLength * index + begin;

        // find index of the file where the 1st byte is located
        int file = 0;
        while (ts.filesEndSizeSums[file] <= la) {
            file++;
            if (file == ts.filesEndSizeSums.length) {
                return false;
            }
        }

        // track amount of data to read wrapping small files
        int lengthLeft = length;

        try {
            // support read from several files
            // if the piece overlaps them all
            do {
                // number of bytes available in  after 'begin' position
                long bytesLeftInFile = ts.filesEndSizeSums[file] - la;
                // position in file to start reading
                long positionInFile = ts.files[file].info.length - bytesLeftInFile;
                // file could be too short, read only allowed number of bytes
                long toRead = Math.min(lengthLeft, bytesLeftInFile);

                // buffer (position, limit) points to the space left
                int limit = buffer.limit();
                buffer.limit(buffer.position() + (int)toRead);

                // will read only if this part is allocated
                ts.files[file].channel.read(buffer, positionInFile);

                // restore limit
                buffer.limit(limit);

                // switch to the next file
                file++;
                // correct linear address to write to and bytes left
                la += toRead;
                lengthLeft -= toRead;
            } while (0 < lengthLeft);
        } catch (IOException ignored)
        {
            // indicate no data available
            buffer.position(0).limit(0);
            return false;
        }

        // prepare buffer for reading
        buffer.position(0).limit(length);

        System.out.println(" [sfs]  _read  exit buf:" + buffer.position() + "," + buffer.limit());

        return true;
    }

    /**
     * TODO: remove OOS
     * @param metainfo
     * @param pieces
     * @param active
     */
    protected void writeState(Metainfo metainfo, BitSet pieces, Map<Integer, Torrent.PieceBlocks> active) {
        try {
            Path state = root.resolve("state");
            Path path = state.resolve(metainfo.infohash.toString());

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bos);
            os.writeObject(pieces);
            os.writeObject(active);
            os.flush();

            if (!Files.exists(state)) {
                Files.createDirectory(state);
            }
            Files.write(path, bos.toByteArray());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    protected void readState(Metainfo metainfo, BitSet pieces, Map<Integer, Torrent.PieceBlocks> active, Consumer<Boolean> callback) {
        try {
            Path path = root.resolve("state/" + metainfo.infohash.toString());
            if (!Files.exists(path)) {
                if (callback != null) {
                    callback.accept(false);
                }
                return;
            }

            byte[] data = Files.readAllBytes(path);

            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));

            BitSet rPieces = (BitSet) ois.readObject();
            pieces.clear();
            pieces.or(rPieces);

            Map<Integer, Torrent.PieceBlocks> rActive = (Map<Integer, Torrent.PieceBlocks>) ois.readObject();
            active.clear();
            rActive.forEach((p, b) -> {
                //b.active.clear();
                active.put(p, b);
                throw new RuntimeException("review & fix");
            });

            if (callback != null) {
                callback.accept(true);
            }

        } catch (Exception e) {
            if (callback != null) {
                callback.accept(false);
            }
        }
    }

    @Override
    public void write(String key, byte[] data) {
        try {
            Path state = root.resolve("state");
            if (!Files.exists(state)) {
                Files.createDirectory(state);
            }

            Path path = state.resolve(key);
            Files.write(path, data,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.SYNC);

        } catch (IOException ignored) {
        }
    }

    @Override
    public byte[] read(String key) {
        try {
            Path state = root.resolve("state");
            if (!Files.exists(state)) {
                return null;
            }

            Path path = state.resolve(key);
            return Files.readAllBytes(path);
        } catch (IOException ignored) {
        }
        return null;
    }

}
