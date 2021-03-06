package oot.be;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * binary encoding value that supports all types inside for simplification,
 * works as simple DTO
 */
public class BEValue {
    /**
     * all known types of binary encoding values
     */
    public enum BEValueType {
        /**
         * integer
         */
        INT,
        /**
         * binary string
         */
        BSTR,
        /**
         * list of values
         */
        LIST,
        /**
         * dictionary of [BSTR, BEValue]
         */
        DICT
    }

    /**
     * type of the current element
     */
    public final BEValueType type;

    /**
     * value for the case type == INT
     */
    public long   integer;
    /**
     * value for the case type == BSTR
     */
    public byte[] bString;
    /**
     * value for the case type == LIST
     */
    public List<BEValue> list;
    /**
     * value for the case type == DICT,
     * we must maintain order, so it's LinkedHashMap
     */
    public LinkedHashMap<String, BEValue> dictionary;


    /**
     * sets up this element and pre-allocates list/dictionary if necessary
     * @param _type type of the element
     */
    public BEValue(BEValueType _type) {
        type = _type;
        switch (type) {
            case LIST : list = new ArrayList<>(); break;
            case DICT : dictionary = new LinkedHashMap<>(); break;
        }
    }

    /**
     * @param value value to compare against
     * @return true if this type is BSTR and byte value is equal to UTF-8 bytes of the value
     */
    public boolean equals(String value) {
        return (type == BEValueType.BSTR) &&
                (Arrays.compare(bString, value.getBytes(StandardCharsets.UTF_8)) == 0);
    }

    /**
     * @param value value to compare against
     * @return true if this type is BSTR and byte value is equal to low byte of char
     */
    public boolean equals(char value) {
        return (type == BEValueType.BSTR) &&
                (bString.length == 1) &&
                (Byte.toUnsignedInt(bString[0]) == (value & 0xFF));
    }

    /**
     * check internal list existence, doesn't check type to be LIST,
     * it's task for the calling party
     * @return true if internal list is missing or empty
     */
    public boolean isListEmpty() {
        return (list == null) || list.isEmpty();
    }

    /**
     * check internal list existence, doesn't check type to be LIST,
     * it's task for the calling party
     * @return true if internal list is present and not empty
     */
    public boolean isListNotEmpty() {
        return (list != null) && !list.isEmpty();
    }

    /**
     * @return true if type of the element is DICT
     */
    public boolean isDict() {
        return type == BEValueType.DICT;
    }

    /**
     * check internal dictionary existence, doesn't check type to be DICT,
     * it's task for the calling party
     * @return true if internal dictionary is present and not empty
     */
    public boolean isDictNotEmpty() {
        return (dictionary != null) && !dictionary.isEmpty();
    }

    /**
     * @return if current value is of integer type
     */
    public boolean isInteger() {
        return (type == BEValueType.INT);
    }

    /**
     * @return bString value as String parsed with UTF-8
     */
    public String getBStringAsString() {
        return new String(bString, StandardCharsets.UTF_8);
    }


    /**
     * @param value parsed binary encoded value
     * @return true if value has integer type
     */
    public static boolean isInteger(BEValue value) {
        return (value != null) && (value.type == BEValueType.INT);
    }

    /**
     * @param value parsed binary encoded value
     * @return true if value has binary string type
     */
    public static boolean isBString(BEValue value) {
        return (value != null) && (value.type == BEValueType.BSTR);
    }

    /**
     * @param value parsed binary encoded value
     * @return true if value has binary string type and string is not null
     */
    public static boolean isBStringNotNull(BEValue value) {
        return isBString(value) && (value.bString != null);
    }

    /**
     * @param value parsed binary encoded value
     * @return true if value has binary string type and string is not empty
     */
    public static boolean isBStringNotEmpty(BEValue value) {
        return isBStringNotNull(value) && (value.bString.length != 0);
    }

    /**
     * @param value parsed binary encoded value
     * @return true if value has binary string type and string has specific length
     */
    public static boolean isBStringWithLength(BEValue value, int length) {
        return isBStringNotNull(value) && (value.bString.length == length);
    }

    /**
     * @param value parsed binary encoded value
     * @return true if value has binary string type and string has at least the specified length
     */
    public static boolean isBStringAtLeast(BEValue value, int length) {
        return isBStringNotNull(value) && (value.bString.length >= length);
    }

    /**
     * @param value parsed binary encoded value
     * @return true if value has dictionary type
     */
    public static boolean isDict(BEValue value) {
        return (value != null) && (value.type == BEValueType.DICT);
    }

    /**
     * @param value parsed binary encoded value
     * @return true if value has dictionary type and dictionary is not null
     */
    public static boolean isDictNotNull(BEValue value) {
        return isDict(value) && (value.dictionary != null);
    }

    /**
     * @param value parsed binary encoded value
     * @return true if value has dictionary type and dictionary is not empty
     */
    public static boolean isDictNotEmpty(BEValue value) {
        return isDictNotNull(value) && !value.dictionary.isEmpty();
    }

    /**
     * @param value parsed binary encoded value
     * @return true if value has list type
     */
    public static boolean isList(BEValue value) {
        return (value != null) && (value.type == BEValueType.LIST);
    }

    /**
     * @param value parsed binary encoded value
     * @return true if value has list type and list is not null
     */
    public static boolean isListNotNull(BEValue value) {
        return isList(value) && (value.list != null);
    }

    /**
     * @param value parsed binary encoded value
     * @return true if value has list type and list is not empty
     */
    public static boolean isListNotEmpty(BEValue value) {
        return isListNotNull(value) && !value.list.isEmpty();
    }

    /**
     * recursively dumps node to stdout
     * @param intent intent for the current dump level
     */
    public void dump(int intent) {
        String prefix = " ".repeat(intent);
        switch (type) {
            case INT:
                System.out.println(prefix + integer);
                break;
            case BSTR:
                System.out.println(prefix + new String(bString, StandardCharsets.UTF_8));
                break;
            case LIST:
                list.forEach( v -> v.dump(intent));
                break;
            case DICT:
                dictionary.forEach( (k, v) -> {
                    System.out.println(prefix + k);
                    v.dump(intent + 2);
                });
                break;
        }
    }
}
