package oot;

import oot.be.Metainfo;
import oot.dht.HashId;
import oot.dht.Node;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Client {

    /**
     * unique 'fixed' identifier of this client
     */
    HashId id;

    // dht node if available
    Node node;

    // list of torrents [or map]
    List<Torrent> torrents;

    ExecutorService executor = new ThreadPoolExecutor(
            2, 16,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>());


    /*
    *
    *  Client 1--* Torrent
    *
    *  concurrent
    *  - torrents
    *     several torrents in 1 thread
    *
    *  - trackers update
    *  - dht update
    *    peers updated into torrent --> sync access to peers
    *    :?  sync(xPeers) peers to internal xPeers, onUpdate (main thread): sync(xPeers) xPeers --> peers
    *
    *  - store ?
    *    copy data?
    *
    *  - read --> enqueue / buffer / timeout / lock
    *
    *
    * */

    void x(Torrent t) {
        t.addPeers(null);
    }

    Storage storage;

    ClientThread thread;

    public Client(HashId _id, Storage _storage /*, Executors?*/) {
        // creates separate thread(s) for processing/updates

        id = _id;
        storage = _storage;

        torrents = new ArrayList<>();



    }

    public boolean start(boolean daemon) {

        try {
            selector = Selector.open();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }


        thread = new ClientThread();
        thread.setDaemon(daemon);
        thread.start();

        return true;
    }

    public void stop(boolean join) {
        thread.active = false;

        storage.stop();

        if (join) {
            try {
                thread.join();
            } catch (InterruptedException ignored) {
            }
        }
    }

    void dht(boolean enable) {}



    Torrent addTorrent(Metainfo info) {
        Storage.TorrentStorage tStorage = this.storage.getStorage(info);
        Torrent torrent = new Torrent(this, info, tStorage);
        torrents.add(torrent);
        return torrent;
    }




    // t.getState()
    // t.stop / start / pause / .. / remove / delete

    //ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    //    executor.scheduleAtFixedRate(() -> {}, 0, 10, TimeUnit.MILLISECONDS);


    Selector selector;

    class ClientThread extends Thread {

        volatile boolean active = true;

        @Override
        public void run() {

            while (active) {

                try {
                    // for all torrents
                    selector.select(sKey -> {
                        PeerConnection p = (PeerConnection) sKey.attachment();
                        p.onChannelReady();
                    }, 10);
                } catch (IOException e) {
                    e.printStackTrace();
                }


                for (Torrent torrent: torrents) {
                    long now = System.currentTimeMillis();
                    if (100 < now - torrent.timeLastUpdate) {
                        torrent.update();
                    }
                    if (4000 < now - torrent.timeLastDump) {
                        torrent.dump();
                    }
                }
            }

        }
    }



}
