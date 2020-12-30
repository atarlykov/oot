package oot;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * threads used to process connections' updates for torrents
 * and other possible commands that must be executed in bounds
 * of a torrents' processing thread.
 *
 * could be considered as the quickest way to send commands to be executed
 * inside a torrent thread, should be used to request processing of selection keys
 * and torrents' updates
 *
 * other commands to a torrent could be send via special queue inside each torrent,
 * such commands will be processed on update commands sent via this class
 */
class TorrentRunnerThread extends Thread
{
    public static boolean DEBUG = true;

    /**
     * collection of queues for each active torrent,
     * must be cleared eventually by client's main thread
     */
    final ConcurrentMap<Torrent, ConcurrentLinkedQueue<TorrentCommand>> runCmdQueues;
    /**
     * marker queue to sequentially notify runner threads
     * about new commands available to be processed for a torrent
     */
    final ConcurrentLinkedQueue<Torrent> runMarkerQueue;


    /**
     * @param runMarkerQueue ref to marker queue
     * @param runCmdQueues ref to command queues
     */
    public TorrentRunnerThread(
            ConcurrentLinkedQueue<Torrent> runMarkerQueue,
            ConcurrentMap<Torrent, ConcurrentLinkedQueue<TorrentCommand>> runCmdQueues)
    {
        super("oot-runner");
        this.runCmdQueues = runCmdQueues;
        this.runMarkerQueue = runMarkerQueue;
    }


    @Override
    public void run()
    {
        while (true)
        {
            // get marker if exists
            Torrent torrent = runMarkerQueue.poll();
            if (torrent != null) {
                // try lock, if unsuccessful - some other runner
                // is processing this torrent after getting another marker,
                // that's ok, let him process the queue
                if (torrent.runnerLock.tryLock()) {
                    // process all commands for this torrent
                    // with lock protection
                    try {
                        ConcurrentLinkedQueue<TorrentCommand> commands = runCmdQueues.get(torrent);
                        if (DEBUG) {
                            //TRCmd peek = commands.peek();
                            //System.out.println(System.nanoTime() + "  [TR] (cycle)  queue size:" + commands.size() +
                            //        (peek != null ? "   " + peek.getClass() : ""));
                        }
                        TorrentCommand command = null;
                        while ((command = commands.poll()) != null) {
                            command.execute(torrent);
                        }
                    } finally {
                        torrent.runnerLock.unlock();
                    }
                }
            } else {
                // marker queue is empty,
                // wait for new events
                synchronized (runMarkerQueue) {
                    try {
                        //System.out.println(System.nanoTime() + "  [TR]  (wait)");
                        long tStart = System.nanoTime();
                        runMarkerQueue.wait(100);
                        long tEnd = System.nanoTime();
                        //System.out.println("[TR] time:" + (tEnd - tStart)/1000000);
                    } catch (InterruptedException ignored) {
                        // seems controlling client wants
                        // to stop this instance
                        break;
                    }
                }
            }

            // separate check in case if queue is always full,
            // but we must stop processing
            if (Thread.interrupted()) {
                break;
            }
        }
    }
}
