package oot.poc;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Map;

/**
 * executes a bunch of torrents
 */
public class TorrentRunner {

    void update() {
        /*
         * selector --> connection.receive --> notify torrent
         * selector --> connection.send  --> process send queue/buffer
         *
         * torrents.update() --> connections.update --> pull job from torrent
         *
         * client.distribute() --> 
         */
    }

    void add(Torrent t) {}
    void remove(Torrent t) {} // by hash


}


