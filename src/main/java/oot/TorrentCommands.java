package oot;

import java.util.function.Consumer;

/**
 * Helper class to store various commands to be send to a torrent,
 * just to not store them inside Torrent class
 */
public class TorrentCommands {
    /**
     * will call peer connection io processing
     */
    public static class CmdPeerConnection implements TorrentCommand
    {
        // ref to peer connection that will be processed
        PeerConnection pc;
        /**
         * allowed constructor
         * @param _pc ref to pc
         */
        public CmdPeerConnection(PeerConnection _pc) {
            pc = _pc;
        }
        @Override
        public void execute() {
            pc.onChannelReady();
        }
    }

    /**
     * will call #{@link Torrent#update()}
     */
    public static class CmdTorrentUpdate implements TorrentCommand
    {
        // ref to torrent that will be processed
        Torrent torrent;

        /**
         * allowed constructor
         * @param _torrent ref to torrent
         */
        public CmdTorrentUpdate(Torrent _torrent) {
            torrent = _torrent;
        }
        @Override
        public void execute() {
            torrent.update();
        }
    }

    /**
     * will call #{@link Torrent#dump()}
     */
    public static class CmdTorrentDump implements TorrentCommand
    {
        // ref to torrent that will be processed
        Torrent torrent;

        /**
         * allowed constructor
         * @param _torrent ref to torrent
         */
        public CmdTorrentDump(Torrent _torrent) {
            torrent = _torrent;
        }
        @Override
        public void execute() {
            torrent.dump();
        }
    }


    /**
     * creates "torrent::start" command for the given torrent
     * and places it into the commands' queue of the torrent
     * to be executed later
     * @param torrent torrent
     */
    public static void cmdTorrentStart(Torrent torrent)
    {
        torrent.addCommand(torrent::cmdStart);
    }

    /**
     * creates "torrent::writeState" command for the given torrent
     * and places it into the commands' queue of the torrent
     * to be executed later
     * @param torrent torrent
     */
    public static void cmdTorrentWriteState(Torrent torrent)
    {
        torrent.addCommand(torrent::writeState);
    }

    /**
     * creates "torrent::readState" command for the given torrent
     * and places it into the commands' queue of the torrent
     * to be executed later
     * @param torrent torrent
     */
    public static void cmdTorrentReadState(Torrent torrent, Consumer<Boolean> callback)
    {
        torrent.addCommand(() -> torrent.readState(callback));
    }

}
