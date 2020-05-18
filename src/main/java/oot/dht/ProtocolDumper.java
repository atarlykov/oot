package oot.dht;

import oot.be.BEValue;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * utility class for dumping DHT protocol messages
 * for debug reasons
 */
public class ProtocolDumper {

    /**
     * appends binary encoded string parsed with UTF-8 to builder,
     * if value is missing appends other string
     * @param builder builder to append
     * @param value value to dump
     * @param other other value for the case of missing value
     */
    private static void dumpString(StringBuilder builder, BEValue value, String other) {
        if (BEValue.isBStringNotNull(value)) {
            builder.append(new String(value.bString, StandardCharsets.UTF_8));
        } else {
            builder.append(other);
        }
    }

    /**
     * appends binary encoded string to builder using HEX representation,
     * if value is missing appends other string
     * @param builder builder to append
     * @param value value to dump
     * @param other other value for the case of missing value
     */
    private static void dumpStringHex(StringBuilder builder, BEValue value, String other) {
        if (BEValue.isBStringNotNull(value)) {
            for (byte b: value.bString) {
                builder.append( Character.toUpperCase(Character.forDigit( (b >> 4) &0xF, 16)));
                builder.append( Character.toUpperCase(Character.forDigit( b & 0xF, 16)) );
            }
        } else {
            builder.append(other);
        }
    }

    /**
     * appends binary encoded string to builder using HEX representation,
     * uses part of bytes [from, to)
     * @param builder builder to append
     * @param binary value to dump
     * @param from from index
     * @param to to index
     */
    private static void dumpStringHex(StringBuilder builder, byte[] binary, int from, int to) {
        if ((binary != null)) {
            to = (binary.length < to) ? binary.length : to;
            for (int i = from; i < to; i++) {
                byte b = binary[i];
                builder.append( Character.toUpperCase(Character.forDigit( (b >> 4) &0xF, 16)));
                builder.append( Character.toUpperCase(Character.forDigit( b & 0xF, 16)) );
            }
        }
    }

    /**
     * appends binary encoded integer to builder,
     * if value is missing appends other string
     * @param builder builder to append
     * @param value value to dump
     * @param other other value for the case of missing value
     */
    private static void dumpInteger(StringBuilder builder, BEValue value, String other) {
        if (BEValue.isInteger(value)) {
            builder.append(value.integer);
        } else {
            builder.append(other);
        }
    }

    /**
     * recursively dumps binary encoded value into string
     * @param value value to dump
     * @param builder builder to append
     */
    private static void dumpRecursivelyAsString(StringBuilder builder, BEValue value) {
        switch (value.type) {
            case INT:
                builder.append(value.integer);
                break;
            case BSTR:
                dumpStringHex(builder, value, "?");
                break;
            case LIST:
                builder.append('[');
                value.list.forEach( v -> {
                    dumpRecursivelyAsString(builder, v);
                    builder.append(',');
                });
                builder.append(']');
                break;
            case DICT:
                builder.append('{');
                value.dictionary.forEach( (k, v) -> {
                    builder.append(k).append("->");
                    dumpRecursivelyAsString(builder, v);
                    builder.append(',');
                });
                builder.append('}');
                break;
        }
    }

    /**
     * dumps DHT message sent or received with message custom formatting
     * @param outbound is it outgoing or received message
     * @param skipped true if message was not really send
     * @param address destination address
     * @param message message
     */
    public static void dumpMessage(boolean outbound, boolean skipped, SocketAddress address, BEValue message)
    {
        BEValue y = message.dictionary.get("y");
        StringBuilder builder = new StringBuilder();
        if (outbound) {
            if (skipped) {
                builder.append(">>XX ");
            } else {
                builder.append(">>-- ");
            }
        } else {
            builder.append("--<< ");
        }

        dumpString(builder, y, "?");
        builder.append(" [").append(address.toString()).append(']');

        if (y == null) {
            // no query type, can't parse
            System.out.println(builder.toString());
            return;
        }

        if (y.equals('e'))
        {
            // this is an error
            builder.append("    ");
            BEValue e = message.dictionary.get("e");
            builder.append( (e != null) && (e.list != null) && (0 < e.list.size()) ? e.list.get(0).integer : "?").append("  ");
            builder.append( (e != null) && (e.list != null) && (1 < e.list.size()) ? new String(e.list.get(1).bString, StandardCharsets.UTF_8) : "?");
            System.out.println(builder.toString());
            return;
        }

        // transaction id
        builder.append('\n');
        builder.append("        tx: ");
        BEValue tx = message.dictionary.get("t");
        dumpStringHex(builder, tx, "?");

        // version (optional)
        BEValue version = message.dictionary.get("v");
        builder.append("    v:");
        dumpStringHex(builder, version, "?");

        builder.append("  ");
        if (y.equals('q')) {
            // query
            builder.append("q:");
            BEValue q = message.dictionary.get("q");
            dumpString(builder, q, "?");
            dumpQueryArguments(builder, q, message.dictionary.get("a"));
        }
        else if (y.equals('r'))  {
            // response
            builder.append("r:");
            BEValue r = message.dictionary.get("r");
            dumpRecursivelyAsString(builder, r);
            dumpResponse(builder, r);
        }
        else {
            // unknown type
            builder.append("?");
        }

        System.out.println(builder.toString());
    }

    /**
     * dumps arguments for all known queries
     * @param builder builder to append
     * @param query query
     * @param a arguments dictionary
     */
    private static void dumpQueryArguments(StringBuilder builder, BEValue query, BEValue a)
    {
        builder.append('\n');
        if ((a == null) || (a.dictionary == null)) {
            builder.append("        arguments are missing");
            return;
        }

        builder.append("                   id:");
        dumpStringHex(builder, a.dictionary.get("id"), "missing");

        if (query.equals("find_node")) {
            builder.append('\n');
            builder.append("               target:");
            dumpStringHex(builder, a.dictionary.get("target"), "missing");
        }
        else if (query.equals("get_peers")) {
            builder.append('\n');
            builder.append("            info_hash:");
            dumpStringHex(builder, a.dictionary.get("info_hash"), "missing");
        }
        else if (query.equals("announce_peer")) {
            builder.append('\n').append("         implied_port:");
            dumpInteger(builder, a.dictionary.get("implied_port"), "?");
            builder.append('\n').append("            info_hash:");
            dumpStringHex(builder, a.dictionary.get("info_hash"), "missing");
            builder.append('\n').append("                 port:");
            dumpInteger(builder, a.dictionary.get("port"), "missing");
            builder.append('\n').append("                token:");
            dumpStringHex(builder, a.dictionary.get("token"), "missing");
        }
    }

    /**
     * dumps a response message
     * @param builder builder to append
     * @param response message
     */
    private static void dumpResponse(StringBuilder builder, BEValue response)
    {
        builder.append('\n');
        if ((response == null) || (response.dictionary == null)) {
            builder.append("        response is missing");
            return;
        }

        /*
         * {""nodes" : "<compact node info>"}
         *
         * {"id" : "<queried nodes id>", "token" :"<opaque write token>", "values" : ["<peer 1 info string>", "<peer 2 info string>"]}
         * {"id" : "<queried nodes id>", "token" :"<opaque write token>", "nodes" : "<compact node info>"}
         *
         * 
         */

        builder.append("                   id:");
        dumpStringHex(builder, response.dictionary.get("id"), "missing");

        BEValue token = response.dictionary.get("token");
        if ((token != null) && (token.bString != null)) {
            builder.append('\n').append("               token:");
            dumpStringHex(builder, token, "missing");
        }

        BEValue nodes = response.dictionary.get("nodes");
        if ((nodes != null) && (nodes.bString != null)) {
            builder.append('\n').append("                nodes:");
            dumpNodesCompactForm(builder, nodes.bString);
        }

        BEValue values = response.dictionary.get("values");
        if ((values != null) && (values.list != null)) {
            builder.append('\n').append("               values:");
            int valuesSize = values.list.size();
            for (int i = 0; i < valuesSize; i++) {
                BEValue e = values.list.get(i);
                if (e.bString != null) {
                    dumpAddressCompactForm(builder, e.bString, 0);
                    if (i < valuesSize - 1) {
                        builder.append('\n').append("                    :");
                    }
                }
            }
        }

    }

    /**
     * parses nodes array represented in the compact form and dumps in it readable form
     * @param builder builder to append
     * @param binary binary representation of nodes
     */
    private static void dumpNodesCompactForm(StringBuilder builder, byte[] binary)
    {
        int count = binary.length / 26;
        for (int i = 0; i < count; i++) {
            // each compact form is 26 bytes
            int index = i * 26;

            if (0 < i) {
                builder.append('\n').append("                      ");
            }
            dumpStringHex(builder, binary, index, index + 20);

            builder.append("  ");
            dumpAddressCompactForm(builder, binary, index + 20);
        }
    }

    /**
     * parses and dumps array with addresses compact representation
     * @param builder builder to append
     * @param binary binary representation of addresses
     * @param index index of the addrass to dump
     */
    private static void dumpAddressCompactForm(StringBuilder builder, byte[] binary, int index) {
        if (binary.length < index + 6) {
            builder.append("length incorrect");
        }
        builder.append(Byte.toUnsignedInt(binary[index + 0])).append('.');
        builder.append(Byte.toUnsignedInt(binary[index + 1])).append('.');
        builder.append(Byte.toUnsignedInt(binary[index + 2])).append('.');
        builder.append(Byte.toUnsignedInt(binary[index + 3]));

        int port = ((Byte.toUnsignedInt(binary[index + 4]) << 8) +
                Byte.toUnsignedInt(binary[index + 5]));
        builder.append(":").append(port);
    }
}
