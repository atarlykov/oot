package oot.be;

import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * binary encoding parser, contains base methods to parse raw data
 * and various helpers for custom torrent related cases
 */
public class BEParser {

    /**
     * reads and parses next element from the stream,
     * ending 'e' token buffer also read if must be
     * @param buffer stream to read from
     * @return parsed and populated element or null if
     * next element buffer 'e' token [support for lists and etc,
     * could be refactored with us of {@link java.io.PushbackInputStream}]
     * @throws IllegalArgumentException if buffer structure corrupted or couldn't be parsed
     * @throws BufferUnderflowException if buffer is underflow
     */
    private BEValue parseElement(ByteBuffer buffer) throws IllegalArgumentException, BufferUnderflowException {

        // check 'type' byte, it could be one of 'idl' characters
        // or digit in case of bstring value
        int bType = buffer.get(buffer.position());

        if (bType == 'i') {
            return _parseInteger(buffer);
        }
        else if (Character.isDigit(bType)) {
            return _parseBString(buffer);
        }
        else if (bType == 'd') {
            // move buffer pointer after element type
            buffer.get();
            BEValue beValue = new BEValue(BEValue.BEValueType.DICT);
            while (true) {
                // check if next token is 'e'/end of dictionary
                int nextToken = Byte.toUnsignedInt(buffer.get(buffer.position()));
                if (nextToken == 'e') {
                    // read this token
                    buffer.get();
                    return beValue;
                }

                BEValue key = parseElement(buffer);
                BEValue subValue = parseElement(buffer);

                // keys are always bstring
                String sKey = new String(key.bString, StandardCharsets.UTF_8);
                beValue.dictionary.put(sKey, subValue);
            }
        }

        else if (bType == 'l') {
            // move buffer pointer after element type
            buffer.get();

            BEValue beValue = new BEValue(BEValue.BEValueType.LIST);

            while (true) {
                // check if next token is 'e'/end of dictionary
                int nextToken = Byte.toUnsignedInt(buffer.get(buffer.position()));
                if (nextToken == 'e') {
                    // read this token
                    buffer.get();
                    return beValue;
                }
                BEValue subValue = parseElement(buffer);
                beValue.list.add(subValue);
            }
        } else {
            // buffer with incorrect data
            throw new IllegalArgumentException("");
        }
    }

    /**
     * parses binary encoded integer together with start/end tokens
     * @param buffer stream to read from
     * @return parsed element
     * @throws IllegalArgumentException if buffer structure corrupted or couldn't be parsed
     * @throws BufferUnderflowException if buffer is underflow
     */
    private BEValue _parseInteger(ByteBuffer buffer) throws IllegalArgumentException, BufferUnderflowException {
        // we are sure this is integer,
        // so skip 'i' token
        buffer.get();

        BEValue beValue = new BEValue(BEValue.BEValueType.INT);
        beValue.integer = 0;
        while (true) {
            int tmp = Byte.toUnsignedInt(buffer.get());
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
     * @param buffer stream to read from
     * @return parsed string
     * @throws IllegalArgumentException if buffer structure corrupted or couldn't be parsed
     * @throws BufferUnderflowException if buffer is underflow
     */
    private BEValue _parseBString(ByteBuffer buffer) throws IllegalArgumentException, BufferUnderflowException {
        BEValue beValue = new BEValue(BEValue.BEValueType.BSTR);

        // parse string length
        int length = 0;
        while (true) {
            int tmp = Byte.toUnsignedInt(buffer.get());
            if (tmp == ':') {
                break;
            }
            if (!Character.isDigit(tmp)) {
                // stream data error
                throw new IllegalArgumentException("");
            }

            length = length * 10 + Character.digit(tmp, 10);
        }

        beValue.bString = new byte[length];

        // parse data
        for (int i = 0; i < length; i++) {
            byte tmp = buffer.get();
            beValue.bString[i] = tmp;
        }

        return beValue;
    }



    /**
     * Parses metainfo, mist be refactored to not depend on parsing
     * assumes all keys are utf8 strings, method fully copies {@link BEParser#parse(ByteBuffer)}
     * with only addition to check for info element
     * @param buffer input stream to parse from
     * @param metainfo metainfo to notify about "info" element to allow digest calculation
     * @return parsed and populated element or null if
     * next element buffer 'e' token [support for lists and etc,
     * could be refactored with us of {@link java.io.PushbackInputStream}]
     * @throws IllegalArgumentException if buffer structure corrupted or couldn't be parsed
     * @throws BufferUnderflowException if buffer is underflow
     */
    public BEValue parse(ByteBuffer buffer, Metainfo metainfo)
            throws IllegalArgumentException, BufferUnderflowException
    {
        // track info element
        int infoStartPosition = -1;
        int infoEndPosition = -1;

        // read type
        int type = Byte.toUnsignedInt(buffer.get(buffer.position()));
        if (type != 'd') {
            // root element must be dictionary
            throw new IllegalArgumentException();
        }

        BEValue data = new BEValue(BEValue.BEValueType.DICT);
        byte[] infohash = null;

        // move buffer pointer after element type
        buffer.get();
        while (true) {
            // check if next token is 'e'/end of dictionary
            int nextToken = Byte.toUnsignedInt(buffer.get(buffer.position()));
            if (nextToken == 'e') {
                // read this token
                buffer.get();
                break;
            }

            BEValue key = parseElement(buffer);
            // keys are always bstring
            String sKey = new String(key.bString, StandardCharsets.UTF_8);

            // mark start position to use in case of "info" element
            infoStartPosition = buffer.position();

            BEValue subValue = parseElement(buffer);
            data.dictionary.put(sKey, subValue);

            if (sKey.equals("info")) {
                // let metainfo calculate digest
                infoEndPosition = buffer.position();
                metainfo.digest(buffer, infoStartPosition, infoEndPosition);
            }
        }


        return data;
    }

    /**
     * Parses common bstring
     * @param buffer stream to read
     * @return decoded element
     * @throws IllegalArgumentException if buffer structure corrupted or couldn't be parsed
     * @throws BufferUnderflowException if buffer is underflow
     */
    public BEValue parse(ByteBuffer buffer) throws IllegalArgumentException, BufferUnderflowException {
        return parseElement(buffer);
    }


    /**
     * Serializes given value into byte array
     * @param value value to serialize
     * @return not null array
     */
    public byte[] encode(BEValue value) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        encodeElement(os, value);
        return os.toByteArray();
    }

    /**
     * Serializes given value into the specified stream
     * @param os stream to serialize into
     * @param value value to serialize
     */
    private void encodeElement(ByteArrayOutputStream os, BEValue value) {

        if (value.type == BEValue.BEValueType.INT) {
            os.write((byte)'i');
            os.writeBytes(Long.toString(value.integer).getBytes(StandardCharsets.UTF_8));
            os.write((byte)'e');
        }
        else if (value.type == BEValue.BEValueType.BSTR) {
            os.writeBytes(Integer.toString(value.bString.length).getBytes(StandardCharsets.UTF_8));
            os.write((byte)':');
            os.writeBytes(value.bString);
        }
        else if (value.type == BEValue.BEValueType.DICT) {
            os.write((byte)'d');
            value.dictionary.forEach((k, v) -> {
                os.writeBytes(Integer.toString(k.length()).getBytes(StandardCharsets.UTF_8));
                os.write((byte)':');
                os.writeBytes(k.getBytes(StandardCharsets.UTF_8));
                System.out.println("d: " + k);
                encodeElement(os, v);
            });
            os.write((byte)'e');
        }
        else if (value.type == BEValue.BEValueType.LIST) {
            os.write((byte)'l');
            value.list.forEach(v -> encodeElement(os, v));
            os.write((byte)'e');
        } else {
            // buffer with incorrect data
            throw new IllegalArgumentException("");
        }
    }

}
