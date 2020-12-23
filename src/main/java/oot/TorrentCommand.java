package oot;

/**
 * base interface to send commands to a torrent from client
 * or via torrent api,
 * needed just to have basic interface and normal class names (not lambda)
 * to be visible in tracing and debug
 */
@FunctionalInterface
public interface TorrentCommand {
    /**
     * main method
     */
    void execute();
}
