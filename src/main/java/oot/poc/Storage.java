package oot.poc;

import oot.be.Metainfo;

/**
 * Storage engine that handles data operations for all torrents
 */
public abstract class Storage
{
    /**
     * size of blocks transferred,
     * used to allocate buffers
     * todo: try to remove
     */
    int blockSize;

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
