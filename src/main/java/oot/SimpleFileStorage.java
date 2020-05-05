package oot;

import oot.be.Metainfo;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import java.util.ArrayDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

/**
 * Storage that allows writing torrent data to dist and reading
 * it back synchronously and without any caching,
 *
 * that usually works fine, but blocks network operations,
 * the same time this prevent client from flooding with requests
 *
 * uses {@link java.util.concurrent.ExecutorService} to run long tasks
 * like disk space allocations and torrent checks
 */
public class SimpleFileStorage extends Storage {

    /**
     * simple internal container to link file information from metainfo
     * to specific file channel to read/write data
     */
    private static class TorrentFile {
        FileChannel channel;
        Metainfo.FileInfo info;
    }

    /**
     * instances of this class will be created for each torrent inside a client,
     * to perform storing and reads of data
     */
    public class SimpleFileTorrentStorage extends TorrentStorage {
        // parent torrent
        //Torrent torrent;
        TorrentFile[] files;
        long[] filesEndSizeSums;


        public SimpleFileTorrentStorage(Path _root, Metainfo _metainfo) {
            metainfo = _metainfo;
            root = _root;
            state = TorrentStorageState.UNKNOWN;

            files = new TorrentFile[metainfo.files.size()];
            filesEndSizeSums = new long[metainfo.files.size()];
        }


        @Override
        public void init(Consumer<Boolean> callback) {
            SimpleFileStorage.this._init(this, preallocate, callback);
        }

        public void init(boolean _preallocate, Consumer<Boolean> callback) {
            SimpleFileStorage.this._init(this, _preallocate, callback);
        }


        @Override
        public ByteBuffer getBuffer() {
            return SimpleFileStorage.this.getBuffer();
        }

        @Override
        public void releaseBuffer(ByteBuffer buffer) {
            SimpleFileStorage.this.releaseBuffer(buffer);
        }

        @Override
        public void check(Consumer<Boolean> callback) {
            super.check(callback);
        }

        @Override
        public void write(ByteBuffer buffer, int index, int begin, int length, Consumer<Boolean> callback) {
            // make copy to run save operation in parallel

            ByteBuffer tmp = getBuffer();
            tmp.put(buffer).flip();

            boolean result = _save(this, tmp, index, begin, length);
            releaseBuffer(tmp);
            if (callback != null) {
                callback.accept(result);
            }
        }

        @Override
        public void read(ByteBuffer buffer, int index, int begin, int length, Consumer<Boolean> callback) {
            boolean result = _read(this, buffer, index, begin, length);
            if (callback != null) {
                callback.accept(result);
            }
        }

    }

    /**
     * common root folder to be used as default root path for all torrents
     */
    private Path root;

    /**
     * do we preallocate space by default
     */
    private boolean preallocate = true;

    /**
     * size of blocks (buffers) we are going to use
     */
    private int blockSize;

    /**
     * cached buffers
     */
    final ArrayDeque<ByteBuffer> cache = new ArrayDeque<>();

    //ThreadPoolExecutor exRead = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
    //ThreadPoolExecutor exSave = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);


    /**
     * allowed constructor
     * @param _root common root folder for storing torrents' data
     * @param _preallocate files will be pre-allocated if true
     * @param _blockSize size of blocks/buffers to use
     */
    public SimpleFileStorage(Path _root, boolean _preallocate, int _blockSize) {
        this.root = _root;
        this.preallocate = _preallocate;
        this.blockSize = _blockSize;
    }

    @Override
    public TorrentStorage getStorage(Metainfo metainfo) {
        return new SimpleFileTorrentStorage(root, metainfo);
    }

    @Override
    public void stop() {
        //exRead.shutdown();
        //exSave.shutdown();
        executor.shutdown();
    }

    static long buffersAllocated = 0;

    ByteBuffer getBuffer() {
        synchronized (cache) {
            ByteBuffer buffer = cache.pollFirst();
            if (buffer == null) {
                buffersAllocated++;
                return ByteBuffer.allocateDirect(blockSize);
            } else {
                return buffer;
            }
        }
    }

    void releaseBuffer(ByteBuffer buffer) {
        synchronized (cache) {
            buffer.clear();
            cache.offerFirst(buffer);
        }
    }

    /**
     * initializes torrents' storage to be used for reading/writing,
     * @param allocate do we want to pre allocate files or not
     * @param callback callback to be notified on finish
     */
    private void _init(SimpleFileTorrentStorage ts, boolean allocate, Consumer<Boolean> callback) {

        // open channels and init files' info
        try {
            _bind(ts);
            ts.state = TorrentStorageState.READY;
        } catch (IOException e) {
            if (callback != null) {
                callback.accept(false);
            }
            return;
        }

        // calculate "end sums" to map pieces to files later
        long sum = 0l;
        for (int i = 0; i < ts.files.length; i++) {
            TorrentFile file = ts.files[i];
            sum += file.info.length;
            ts.filesEndSizeSums[i] = sum;
        }

        if (allocate) {
            executor.submit(() -> {
                try {
                    ts.state = TorrentStorageState.ALLOCATING;
                    _allocate(ts);
                    ts.state = TorrentStorageState.READY;
                    // notify
                    callback.accept(true);
                } catch (IOException e) {
                    if (callback != null) {
                        callback.accept(false);
                    }
                    return;
                }
            });
        }
    }


    /**
     * opens channels for all files of the parent torrent
     * @throws IOException if any
     */
    private void _bind(SimpleFileTorrentStorage ts) throws IOException {
        Metainfo metainfo = ts.metainfo;

        if (metainfo.isMultiFile()) {
            int index = 0;
            for (Metainfo.FileInfo fileInfo: metainfo.files) {
                TorrentFile tFile = new TorrentFile();
                tFile.info = fileInfo;

                Path path = Paths.get(metainfo.directory, (String[]) fileInfo.names.toArray());
                Path full = root.resolve(path);

                tFile.channel = _bind(full, fileInfo);
                ts.files[index++] = tFile;
            }
        } else {
            Metainfo.FileInfo fileInfo = metainfo.files.get(0);
            String[] names = (String[]) fileInfo.names.toArray(String[]::new);
            Path path = Paths.get(root.toString(), names);

            TorrentFile tFile = new TorrentFile();
            tFile.info = fileInfo;
            tFile.channel = _bind(path, fileInfo);
            ts.files[0] = tFile;
        }
    }

    /**
     * initializes file channel for the given file path
     * @param path full path the file we must open
     * @param fileInfo file info from the torrent
     * @return opens channel
     * @throws IOException if case of any error
     */
    private FileChannel _bind(Path path, Metainfo.FileInfo fileInfo) throws IOException {
        File file = path.toFile();

        if (!file.exists()) {
            FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            return channel;
        }
        else {
            FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);

            if (file.length() == fileInfo.length) {
                return channel;
            }
            else if (file.length() < fileInfo.length) {
                return channel;
            }
            else {
                // that's strange, unknown file?
                return channel;
            }
        }
    }

    /**
     * allocates space for all files
     * @throws IOException if any
     */
    private void _allocate(SimpleFileTorrentStorage ts) throws IOException {
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
    private boolean _save(SimpleFileTorrentStorage ts, ByteBuffer buffer, int index, int begin, int length) {
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
    private boolean _read(SimpleFileTorrentStorage ts, ByteBuffer buffer, int index, int begin, int length) {
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

        try {
            // support read from several files
            // if the piece overlaps them all
            do {
                // number of bytes available in  after 'begin' position
                long bytesLeftInFile = ts.filesEndSizeSums[file] - la;
                // position in file to start reading
                long positionInFile = ts.files[file].info.length - bytesLeftInFile;
                // file could be too short, read only allowed number of bytes
                long toRead = Math.min(length, bytesLeftInFile);

                // buffer has (position, limit) that points to the data left
                int limit = buffer.limit();
                buffer.limit(buffer.position() + (int)toRead);

                ts.files[file].channel.read(buffer, positionInFile);

                // restore limit
                buffer.limit(limit);

                // switch to the next file
                file++;
                // correct linear address to write to and bytes left
                la += toRead;
                length -= toRead;
            } while (0 < length);
        } catch (IOException ignored) {
            return false;
        }

        return true;
    }
}
