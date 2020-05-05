package oot.be;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * binary encoding parser, contains base methods to parse raw data
 * and various helpers for custom torrent related cases
 */
public class BEParserIS {

    /**
     * reads and parses next element from the stream,
     * ending 'e' token is also read if must be
     * @param is stream to read from
     * @return parsed and populated element or null if
     * next element is 'e' token [support for lists and etc,
     * could be refactored with us of {@link java.io.PushbackInputStream}]
     * @throws IllegalArgumentException if stream structure is corrupted or couldn't be parsed
     * @throws IOException or stream read errors
     */
    private BEValue parseElement(InputStream is) throws IllegalArgumentException, IOException {

        // read 'type' byte, it could be one of 'idl' characters
        // or digit in case of bstring value
        int bType = is.read();

        if (bType == 'i') {
            return _parseInteger(is);
        }
        else if (Character.isDigit(bType)) {
            return _parseBString(is, bType);
        }
        else if (bType == 'd') {
            BEValue beValue = new BEValue(BEValue.BEValueType.DICT);
            while (true) {
                // key is always bstring
                BEValue key = parseElement(is);
                if (key == null) {
                    // end of dictionary ['e' token]
                    return beValue;
                }

                BEValue subValue = parseElement(is);
                if (subValue == null) {
                    // stream structure corrupted
                    throw new IllegalArgumentException("");
                }

                String sKey = new String(key.bString, StandardCharsets.UTF_8);
                beValue.dictionary.put(sKey, subValue);
            }
        }

        else if (bType == 'l') {
            BEValue mValue = new BEValue(BEValue.BEValueType.LIST);
            BEValue subValue;
            while ((subValue = parseElement(is)) != null) {
                mValue.list.add(subValue);
            }

            return mValue;
        }

        else if (bType == 'e') {
            // this is hack to parse recursively structures from stream
            // could use PushbackInputStream
            return null;
        } else {
            // steam with incorrect data
            throw new IllegalArgumentException("");
        }
    }

    /**
     * parses binary encoded integer together with end token
     * @param is stream to read from
     * @return parsed element
     * @throws IllegalArgumentException if stream structure is corrupted or couldn't be parsed
     * @throws IOException or stream read errors
     */
    private BEValue _parseInteger(InputStream is) throws IllegalArgumentException, IOException {
        BEValue beValue = new BEValue(BEValue.BEValueType.INT);
        beValue.integer = 0;
        while (true) {
            int tmp = is.read();
            if (tmp == -1) {
                // stream structure error
                throw new IllegalArgumentException("");
            }
            if (tmp == 'e') {
                return beValue;
            }
            if (!Character.isDigit(tmp)) {
                // stream data error
                throw new IllegalArgumentException("");
            }
            beValue.integer = beValue.integer * 10 + Character.digit(tmp, 10);
        }
    }

    /**
     * parses binary encoded string
     * @param is stream to read from
     * @param firstLenDigit first byte of string length field [@see {@link java.io.PushbackInputStream}]
     * @return parsed string
     * @throws IllegalArgumentException if stream structure is corrupted or couldn't be parsed
     * @throws IOException or stream read errors
     */
    private BEValue _parseBString(InputStream is, int firstLenDigit) throws IllegalArgumentException, IOException {
        BEValue beValue = new BEValue(BEValue.BEValueType.BSTR);

        // parse string length
        int length = Character.digit(firstLenDigit, 10);
        while (true) {
            int tmp = is.read();
            if (tmp == -1) {
                // stream structure error
                throw new IllegalArgumentException("");
            }
            else if (tmp == ':') {
                break;
            }
            else if (!Character.isDigit(tmp)) {
                // stream data error
                throw new IllegalArgumentException("");
            }

            length = length * 10 + Character.digit(tmp, 10);
        }

        beValue.bString = new byte[length];

        // parse data
        for (int i = 0; i < length; i++) {
            int tmp = is.read();
            if (tmp == -1) {
                // stream structure error
                throw new IllegalArgumentException("");
            }
            else {
                beValue.bString[i] = (byte)tmp;
            }
        }

        return beValue;
    }


    /**
     * Parses common bstring
     * @param _is stream to read
     * @return decoded element
     * @throws Exception if any
     */
    public BEValue parse(InputStream _is) throws Exception {
        return parseElement(_is);
    }
}
