package oot.dht;

import java.util.Arrays;

public class HashId implements Comparable {
    /**
     *
     * 20 bytes: infohash + nodeid
     * distance == xor, smaller --> closer
     *
     * ask known for peers:
     *   know: return peers
     *   dont: return closer nodes
     * repeat
     *
     * insert itself into nodes closest to hashinfo
     *
     *
     * ask for peers:
     *   return token
     *
     * announce itself must include token
     *
     *
     * routing table:
     *   node is good: responded within last 15 min / or send a query to us
     *     after 15 min inactivity --> questionable
     *     dons't respond to 'multiple' queries --> bad
     *     good nodes have priority
     *
     * range: min=0 max=2**160
     * each bucket holds K nodes(8)
     * when bucket
     *   is full, no more GOOD nodes could be added
     *   but if our ID is inside it, divide on 2 buckets of the same size and distribute nodes
     *      [full bucket is split to 0..2**159  and 2**159..2**160]
     *
     *  track bucket update time (node add, update status, replace)
     *
     *  table MUST be saved between invocations of a program!!!
     */

    public static final int HASH_LENGTH_BYTES = 20;
    public static final int HASH_LENGTH_BITS =  8 * HASH_LENGTH_BYTES;

    /**
     * data bytes
     */
    byte[] data;

    /**
     * allowed constructor
     */
    public HashId() {
        data = new byte[HASH_LENGTH_BYTES];
    }

    /**
     * allowed constructor
     * @param id original array with id
     * @param wrap true if we can wrap and use the specified array
     */
    public HashId(byte[] id, boolean wrap) {
        if ((id == null) || (id.length < HASH_LENGTH_BYTES)) {
            throw new RuntimeException();
        }
        if (wrap) {
            data = id;
        } else {
            data = Arrays.copyOfRange(id, 0, HASH_LENGTH_BYTES);
        }
    }

    /**
     * constructs hash from inside another array, coping data
     * @param binary array with hash inside
     * @param index index in the array with hash data
     * @throws ArrayIndexOutOfBoundsException if array it too short
     */
    public HashId(byte[] binary, int index) {
        data = Arrays.copyOfRange(binary, index, index + HASH_LENGTH_BYTES);
    }

    /**
     * copies data into internal array
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
    public int compareTo(Object o) {
        if (o == null) {
            return -1;
        }
        if (!(o instanceof HashId)) {
            throw new ClassCastException();
        }

        return compare((HashId) o);
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
     * bits are indexed from left to right, from upper bit to lower
     * @param index zero based index
     * @return bit state with the specified index
     */
    public boolean getBit(int index) {
        if (HASH_LENGTH_BITS <= index) {
            return false;
        }
        return (((int) data[index >> 3]) & (1 << (7 - (index & 0b111)))) != 0;
    }

    /**
     * bits are indexed from left to right, from upper bit to lower
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
