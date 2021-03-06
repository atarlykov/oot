package oot.be;


import oot.dht.HashId;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Torrents meta information as usually read from .torrent files
 */
public class Metainfo
{
    /**
     * digest size in bytes for internal validations
     */
    static final int DIGEST_SHA1_LENGTH = 20;

    /**
     * information about a file in meta info
     */
    public static class FileInfo {
        // length of file in bytes
        public long length;
        // path elements
        public List<String> names;

        public FileInfo(long length, List<String> names) {
            this.length = length;
            this.names = names;
        }
    }


    /**
     * raw parsed meta info data as dictionary
     */
    private BEValue data;

    /**
     * SHA-1 digest of the 'info' value,
     * calculated during metainfo parsing
     */
    public final HashId infohash;

    /**
     * total bytes in torrent's data
     */
    public final long length;
    /**
     * size of each piece in bytes
     */
    public final long pieceLength;
    /**
     * total number of pieces,
     * calculated from length and pieceLength
     */
    public final long pieces;
    /**
     * SHA1 hashes of all the pieces,
     * size must be DIGEST_SHA1_LENGTH * pieces
     */
    public final byte[] hashes;

    /**
     * urls from announce-list if present
     * or just announce if not
     */
    public final List<List<String>> trackers = new ArrayList<>();

    /**
     * in case of multi-file mode this holds name
     * of the root directory (suggestion)
     */
    public final String directory;

    /**
     * list of files in metainfo
     */
    public final List<FileInfo> files = new ArrayList<>();


    /**
     * called by parser to calculate digest from the buffer
     * @param buffer buffer with data
     * @param start start position for calculation
     * @param end end position (exclusive)
     * @throws RuntimeException if SHA-1 is not available
     */
    private byte[] digest(ByteBuffer buffer, int start, int end)
    {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-1");
            do {
                digest.update(buffer.get(start++));
            } while (start < end);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 is not available", e);
        }

        return digest.digest();
    }


    /**
     * service DTO class to simplify final semantics of metainfo fields,
     * used during construction
     */
    private static class TempData {
        private HashId infohash;
        public long length;
        public long pieceLength;
        public long pieces;
        public byte[] hashes;
        public List<List<String>> trackers = new ArrayList<>();
        public String directory;
        public List<Metainfo.FileInfo> files = new ArrayList<>();
    }

    /**
     * parses buffer and calculates infohash
     * @param buffer to read data from
     * @throws IllegalArgumentException if data could not be parsed or logically invalid
     */
    public Metainfo(ByteBuffer buffer)
    {
        // temp storage
        TempData x = new TempData();

        BEParser parser = new BEParser();
        data = parser.parse(buffer, Set.of("info"), (String name, ByteBuffer tmp, int from, int to) -> {
            byte[] digest = digest(tmp, from, to);
            x.infohash = HashId.wrap(digest);
        });
        populate(x);

        // copy temp to final fields
        infohash = x.infohash;
        length = x.length;
        pieceLength = x.pieceLength;
        pieces = x.pieces;
        hashes = x.hashes;
        directory = x.directory;
        files.addAll(x.files);
        trackers.addAll(x.trackers);

        if (!validate()) {
            throw new IllegalArgumentException("torrent data is invalid");
        }
    }

    /**
     * @return true if this torrent contains more than one file
     */
    public boolean isMultiFile() {
        return files.size() > 1;
    }


    /**
     * checks meta information to be correct
     * @return true if ok
     */
    public boolean validate()
    {
        if (length <= 0) {
            return false;
        }
        if ((pieceLength <= 0) || (length < pieceLength)) {
            return false;
        }
        if (pieces < 1) {
            return false;
        }
        if ((hashes == null) || (hashes.length != DIGEST_SHA1_LENGTH * pieces)) {
            return false;
        }
        //todo: validate all

        return true;
    }

    /**
     * @param piece piece index
     * @return true if specified piece is the last
     */
    public boolean isLastPiece(int piece) {
        return piece == pieces - 1;
    }

    /**
     * @param piece piece index
     * @return length of the piece specified in bytes, calculates size of the last piece
     */
    public long pieceLength(int piece)
    {
        if (piece < pieces - 1) {
            return pieceLength;
        }
        if (piece == pieces - 1) {
            return length % pieceLength;
        }
        return 0;
    }

    /**
     * validates hash of specified piece to be equal to the specified hash
     * @param piece piece to validate
     * @param hash hash to compare
     * @return true if equals
     */
    public boolean checkPieceHash(int piece, byte[] hash)
    {
        if ((pieces <= piece) || (hash.length != DIGEST_SHA1_LENGTH)) {
            return false;
        }

        return Arrays.equals(hashes, DIGEST_SHA1_LENGTH * piece, DIGEST_SHA1_LENGTH * piece + DIGEST_SHA1_LENGTH,
                hash, 0, DIGEST_SHA1_LENGTH);
    }


    /**
     * service method to extract values from hierarchical BEValue dictionaries
     * @param path path with dots as separators
     * @return value of null if not found
     */
    protected BEValue getValue(String path) {
        String[] split = path.split("\\.");
        BEValue dict = data;
        for (int i = 0; i < split.length; i++) {
            dict = dict.dictionary.get(split[i]);
            if (dict == null) {
                return null;
            }
        }
        return dict;
    }


    /**
     * populates this meta information from the decoded BE data,
     * hardly uses all keys from the specification
     */
    private void populate(TempData temp)
    {
        BEValue beName = getValue("info.name");
        String name = new String(beName.bString, StandardCharsets.UTF_8);

        BEValue value = getValue("info.length");
        if (value != null) {
            // single file mode
            temp.length = value.integer;
            files.add(new FileInfo(value.integer, List.of(name)));
            temp.directory = null;
        } else {
            // multi file mode
            temp.length = 0;
            temp.directory = name;
            BEValue beFiles = getValue("info.files");
            for (BEValue beFile: beFiles.list) {
                BEValue beLength = beFile.dictionary.get("length");
                BEValue bePath = beFile.dictionary.get("path");
                temp.files.add(new FileInfo(
                        beLength.integer,
                        bePath.list.stream()
                                .map(be -> new String(be.bString, StandardCharsets.UTF_8))
                                .collect(Collectors.toList())
                ));
                temp.length += beLength.integer;
            }
        }

        value = getValue("info.piece length");

        if (BEValue.isInteger(value)) {
            temp.pieceLength = value.integer;
        }

        if (0 < temp.pieceLength) {
            temp.pieces = (temp.length + temp.pieceLength - 1) / temp.pieceLength;
        }

        value = getValue("info.pieces");
        if (BEValue.isBStringWithLength(value, 20 * (int)temp.pieces)) {
            temp.hashes = value.bString;
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
                    temp.trackers.add(urls);
                }
            }
        } else {
            BEValue announce = getValue("announce");
            if (BEValue.isBString(announce)) {
                temp.trackers.add(List.of(new String(announce.bString, StandardCharsets.UTF_8)));
            }
        }
    }
}
