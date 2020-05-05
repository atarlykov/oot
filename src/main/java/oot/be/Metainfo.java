package oot.be;


import oot.dht.HashId;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Metainfo {
    /**
     * raw parsed metainfo  data as dictionary
     */
    private BEValue data;
    /**
     * SHA-1 digest of the 'info' value,
     * calculated during metainfo parsing
     */
    private HashId infohash;

    public long length;
    public long pieceLength;
    public long pieces;

    // from announce url(s)
    /**
     * urls from announce-list if present
     * or just announce if not
     */
    public List<List<String>> trackers = new ArrayList<>();
    

    /**
     * information about all files in metainfo
     */
    public static class FileInfo {
        // length of file in bytes
        public long length;
        // path elements
        public List<String> names = new ArrayList<>();

        public FileInfo(long length, List<String> names) {
            this.length = length;
            this.names = names;
        }
    }

    /**
     * in case of multi-file mode this holds name
     * of the root directory (suggestion)
     */
    public String directory = "";

    /**
     * list of files in metainfo
     */
    public List<FileInfo> files = new ArrayList<>();


    /**
     * parses buffer and calculates infohash
     * @param buffer
     */
    public Metainfo(ByteBuffer buffer) {
        BEParser parser = new BEParser();
        data = parser.parse(buffer, this);
        populate();
    }

    /**
     * called by parser to calculate digest from the buffer
     * @param buffer buffer with data
     * @param start start position for calculation
     * @param end end position (exclusive)
     * @throws RuntimeException if SHA-1 is not available
     */
    void digest(ByteBuffer buffer, int start, int end) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-1");
            do {
                digest.update(buffer.get(start++));
            } while (start < end);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 is not available, can't work without it", e);
        }

        infohash = HashId.wrap(digest.digest());
    }

    // checks required data
    public boolean validate() {
        if (length <= 0) {
            return false;
        }
        if ((pieceLength <= 0) || (length < pieceLength)) {
            return false;
        }

        //todo: validate all

        return true;
    }

    private void populate() {
        BEValue value = getValue("info.length");
        if ((value != null) && (value.isInteger())) {
            length = value.integer;
        }

        value = getValue("info.piece length");
        if ((value != null) && (value.isInteger())) {
            pieceLength = value.integer;
        }

        if (0 < pieceLength) {
            pieces = (length + pieceLength - 1) / pieceLength;
        }

        BEValue beName = getValue("info.name");
        String name = new String(beName.bString, StandardCharsets.UTF_8);

        value = getValue("info.length");
        if (value != null) {
            // single file mode
            files.add(new FileInfo(value.integer, List.of(name)));
            directory = null;
        } else {
            // multi file mode
            directory = name;
            BEValue beFiles = getValue("info.files");
            for (BEValue beFile: beFiles.list) {
                BEValue beLength = beFile.dictionary.get("length");
                BEValue bePath = beFile.dictionary.get("path");
                files.add(new FileInfo(
                        beLength.integer,
                        bePath.list.stream()
                                .map(be -> new String(be.bString, StandardCharsets.UTF_8))
                                .collect(Collectors.toList())
                ));
            }
        }

        value = getValue("announce-list");
        if (BEValue.isList(value)) {
            for (BEValue beTrackerUrls: value.list) {
                List<String> urls = new ArrayList<>();
                if (!BEValue.isList(beTrackerUrls)) {
                    continue;
                }
                for (BEValue beTrackerUrl: beTrackerUrls.list) {
                    if (BEValue.isBString(beTrackerUrl)) {
                        urls.add(beTrackerUrl.getBStringAsString());
                    }
                }
                if (!urls.isEmpty()) {
                    trackers.add(urls);
                }
            }
        } else {
            trackers.add(List.of(getAnnounce()));
        }
    }

    public HashId getInfohash() {
        return infohash;
    }

//    public MValue get(String name) {
//    }
//
//    public MValue get(String name, int index) {
//    }

    public String getAnnounce() {
        BEValue value = getValue("announce");
        return new String(value.bString, StandardCharsets.UTF_8);
    }


    public BEValue getValue(String path) {
        String[] split = path.split("\\.");
        BEValue dict = data;
        for (int i = 0; i < split.length; i++) {
            BEValue beValue = dict.dictionary.get(split[i]);
            dict = beValue;
        }
        return dict;
    }

    public boolean isMultiFile() {
        return files.size() > 1;
    }
}
