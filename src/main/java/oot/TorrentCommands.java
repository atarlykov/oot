package oot;

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
        public void execute(Torrent torrent) {
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
        public void execute(Torrent torrent) {
            torrent.update();
        }
    }

}
