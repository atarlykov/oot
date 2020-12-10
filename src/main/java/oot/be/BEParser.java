package oot.be;

import java.io.ByteArrayOutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Set;

/**
 * binary encoding parser, contains base methods to parse raw data
 * and various helpers for custom torrent related cases
 */
public class BEParser {

    /**
     * Functional interface callback to be notified
     * for additional handling of parsed elements
     */
    @FunctionalInterface
    public interface ElementCallback {
        /**
         * @param name key of the element (dictionary elements only)
         * @param buffer buffer with data, must no change state
         * @param from start index in the buffer, inclusive
         * @param to end index, exclusive
         */
        void element(String name, ByteBuffer buffer, int from, int to);
    }

    /**
     * Parses next element from the buffer,
     * ending 'e' token in buffer also read if must be
     *
     * next element buffer 'e' token [support for lists and etc,
     * could be refactored with use of {@link java.io.PushbackInputStream}]
     * @param buffer buffer to read from starting from the position
     * @param cbElements collection of elements to be notified about
     * @param cb callback to be called for cbElements found
     * @return parsed and populated element
     *
     * @throws IllegalArgumentException if buffer structure corrupted or couldn't be parsed
     * @throws BufferUnderflowException if buffer is underflow
     */
    private BEValue parseElement(ByteBuffer buffer, Collection<String> cbElements, ElementCallback cb)
            throws IllegalArgumentException, BufferUnderflowException
    {
        // check 'type' byte, it could be one of 'idl' characters
        // or digit in case of bstring value
        int bType = buffer.get(buffer.position());

        if (bType == 'i') {
            return parseInteger(buffer);
        }
        else if (Character.isDigit(bType)) {
            return parseBString(buffer);
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

                // keys are always of type bstring
                BEValue key = parseElement(buffer, cbElements, cb);
                String sKey = new String(key.bString, StandardCharsets.UTF_8);

                // mark start position to use in case if element of interest
                int infoStartPosition = buffer.position();

                BEValue subValue = parseElement(buffer, cbElements, cb);
                beValue.dictionary.put(sKey, subValue);

                // only call for the specified elements
                if ((cb != null) && (cbElements != null) && cbElements.contains(sKey))
                {
                    int infoEndPosition = buffer.position();
                    // callback must not change buffer state,
                    // but we could fix if necessary
                    cb.element(sKey, buffer, infoStartPosition, infoEndPosition);
                }
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
                BEValue subValue = parseElement(buffer, cbElements, cb);
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
    private BEValue parseInteger(ByteBuffer buffer) throws IllegalArgumentException, BufferUnderflowException {
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
    private BEValue parseBString(ByteBuffer buffer) throws IllegalArgumentException, BufferUnderflowException
    {
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
     * Parses buffer and return BE element if any
     *
     * @param buffer buffer to read from starting from the position
     * @param cbElements collection of elements to be notified about
     * @param cb callback to be called for cbElements found
     * @return parsed and populated element

     * @throws IllegalArgumentException if buffer structure corrupted or couldn't be parsed
     * @throws BufferUnderflowException if buffer is underflow
     */
    public BEValue parse(ByteBuffer buffer, Set<String> cbElements, ElementCallback cb)
            throws IllegalArgumentException, BufferUnderflowException
    {
        return parseElement(buffer, cbElements, cb);
    }

    /**
     * Parses buffer and return BE element if any
     *
     * @param buffer buffer to read from starting from the position
     * @return parsed and populated element

     * @throws IllegalArgumentException if buffer structure corrupted or couldn't be parsed
     * @throws BufferUnderflowException if buffer is underflow
     */
    public BEValue parse(ByteBuffer buffer) throws IllegalArgumentException, BufferUnderflowException {
        return parseElement(buffer, null, null);
    }


    /**
     * Serializes given BE value into byte array
     * @param value value to serialize
     * @return not null array with data in BE format
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
     * @throws IllegalArgumentException if value has unknown type
     */
    private void encodeElement(ByteArrayOutputStream os, BEValue value)
    {
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
            // unknown type
            throw new IllegalArgumentException("");
        }
    }

}
