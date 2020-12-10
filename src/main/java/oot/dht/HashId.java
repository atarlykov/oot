package oot.dht;

import java.util.Arrays;

/**
 * Implements unique 20 bytes hash that is used as nodes' and torrents' identifiers.
 * Distance between tho hashes is calculated as xor(a, b), where resulting
 * 20 bytes binary value considered as distance.
 * todo: move out of dht package, initially was placed here as dht was the full app
 */
public class HashId implements Comparable<HashId> {
    /**
     * size of the hash (fixed)
     */
    public static final int HASH_LENGTH_BYTES = 20;
    public static final int HASH_LENGTH_BITS =  8 * HASH_LENGTH_BYTES;

    /**
     * data bytes of {@link HashId#HASH_LENGTH_BYTES} length
     */
    final byte[] data;

    /**
     * allowed constructor,
     * initializes new zero filled hash
     */
    public HashId() {
        data = new byte[HASH_LENGTH_BYTES];
    }

    /**
     * allowed constructor,
     * builds hash from teh specified data array
     * @param id array with hash data, must be at least of {@link HashId#HASH_LENGTH_BYTES} bytes size
     * @param wrap true if we can wrap and internally use the specified array with hash data
     * @throws IllegalArgumentException if data array is too short
     */
    public HashId(byte[] id, boolean wrap) {
        if (id == null) {
            throw new IllegalArgumentException("data has incorrect size for a hash");
        }
        if (wrap) {
            if (id.length != HASH_LENGTH_BYTES) {
                throw new IllegalArgumentException("data has incorrect size for a hash");
            }
            data = id;
        } else {
            if (id.length < HASH_LENGTH_BYTES) {
                throw new IllegalArgumentException("data has incorrect size for a hash");
            }
            data = Arrays.copyOfRange(id, 0, HASH_LENGTH_BYTES);
        }
    }

    /**
     * constructs hash from data inside another array, performs data copy
     * @param binary array with hash inside
     * @param index index in the array with hash data
     * @throws ArrayIndexOutOfBoundsException if the specified binary array it too short
     */
    public HashId(byte[] binary, int index) {
        if (binary == null) {
            throw new IllegalArgumentException("data has incorrect size for a hash");
        }
        data = Arrays.copyOfRange(binary, index, index + HASH_LENGTH_BYTES);
    }

    /**
     * constructs hash making copy of the data specified
     * @param id id to copy
     */
    public HashId(byte[] id) {
        this(id, false);
    }

    /**
     * wrap specified byte array as internal hash,
     * just facade to improve code readability
     * @param id hash to wrap
     * @return new hash with the wrapped byte array
     */
    public static HashId wrap(byte[] id) {
        return new HashId(id, true);
    }

    /**
     * @return new hash with all elements set to zero
     */
    public static HashId zero() {
        return new HashId();
    }

    /**
     * @param id id array to validate
     * @return return true if the specified array is valid to create a hash object from it
     */
    public static boolean validate(byte[] id) {
        return ((id != null) && (id.length == HASH_LENGTH_BYTES));
    }

    @Override
    public int compareTo(HashId o) {
        return compare(o);
    }

    /**
     * compares two hash ids base on unsigned values of each byte
     * @param other hash to compare with
     * @return see {@link java.util.Comparator#compare(Object, Object)}
     */
    public int compare(HashId other) {
        if (other == null) {
            return -1;
        }
        if (this == other) {
            return 0;
        }

        for (int i = 0; i < HASH_LENGTH_BYTES; i++) {
            int tmp = Byte.compareUnsigned(data[i], other.data[i]);
            if (tmp != 0) {
                return tmp;
            }
        }

        return 0;
    }

    /**
     * calculates XOR distance between two ids
     * @param other other id
     * @return new id that is distance between two
     */
    public HashId distance(HashId other) {
        HashId distance = new HashId();
        for (int i = 0; i < HASH_LENGTH_BYTES; i++) {
            distance.data[i] = (byte)(data[i] ^ other.data[i]);
        }
        return distance;
    }

    /**
     * Returns the specified bit from the hash,
     * bits are indexed from left to right, from the upper bit to the lower one
     * @param index zero based index, [0..HASH_LENGTH_BITS)
     * @return bit state with the specified index, always returns false if index is too large
     */
    public boolean getBit(int index) {
        if (HASH_LENGTH_BITS <= index) {
            return false;
        }
        return (((int) data[index >> 3]) & (1 << (7 - (index & 0b111)))) != 0;
    }

    /**
     * Sets the specified bit in the hash,
     * bits are indexed from left to right, from upper bit to lower.
     * Safely returns if index is too large.
     * @param index zero based index
     * @param value value to set
     */
    public void setBit(int index, boolean value) {
        if (HASH_LENGTH_BITS <= index) {
            return;
        }

        int mask = 1 << (7 - (index & 0b111));
        if (value) {
            data[index >> 3] |= mask;
        } else {
            data[index >> 3] &= ~mask;
        }
    }

    /**
     * @return raw hash bytes
     */
    public byte[] getBytes() {
        return data;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HashId hashId = (HashId) o;
        return Arrays.equals(data, hashId.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(HASH_LENGTH_BYTES * 2);
        for (byte b: data) {
            int value = Byte.toUnsignedInt(b);
            builder.append(Character.forDigit((value >> 4) & 0xF, 16));
            builder.append(Character.forDigit(value & 0xF, 16));
        }
        return builder.toString();
    }
}
