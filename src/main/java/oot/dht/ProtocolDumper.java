package oot.dht;

import oot.be.BEValue;

import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;

public class ProtocolDumper {

    private static void dumpString(StringBuilder builder, BEValue value, String other) {
        if ((value != null) && (value.bString != null)) {
            builder.append(new String(value.bString, StandardCharsets.UTF_8));
        } else {
            builder.append(other);
        }
    }

    private static void dumpStringHex(StringBuilder builder, BEValue value, String other) {
        if ((value != null) && (value.bString != null)) {
            for (byte b: value.bString) {
                builder.append( Character.toUpperCase(Character.forDigit( (b >> 4) &0xF, 16)));
                builder.append( Character.toUpperCase(Character.forDigit( b & 0xF, 16)) );
            }
        } else {
            builder.append(other);
        }
    }

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

    private static void dumpInteger(StringBuilder builder, BEValue value, String other) {
        if ((value != null)) {
            builder.append(value.integer);
        } else {
            builder.append(other);
        }
    }

    private static void dumpAsString(BEValue value, StringBuilder builder) {
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
                    dumpAsString(v, builder);
                    builder.append(',');
                });
                builder.append(']');
                break;
            case DICT:
                builder.append('{');
                value.dictionary.forEach( (k, v) -> {
                    builder.append(k).append("->");
                    dumpAsString(v, builder);
                    builder.append(',');
                });
                builder.append('}');
                break;
        }
    }

    /**
     *
     * dumps message sent or received
     * @param message message
     */
    public static void dumpMessage(boolean outbound, boolean skipped, SocketAddress address, BEValue message) {

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
            System.out.println(builder.toString());
            return;
        }

        if (y.equals('e')) {
            builder.append("    ");
            BEValue e = message.dictionary.get("e");
            builder.append( (e != null) && (e.list != null) && (0 < e.list.size()) ? e.list.get(0).integer : "?").append("  ");
            builder.append( (e != null) && (e.list != null) && (1 < e.list.size()) ? new String(e.list.get(1).bString, StandardCharsets.UTF_8) : "?");
            System.out.println(builder.toString());
            return;
        }

        builder.append('\n');
        builder.append("        tx: ");
        BEValue tx = message.dictionary.get("t");
        dumpStringHex(builder, tx, "?");

        BEValue version = message.dictionary.get("v");
        builder.append("    v:");
        dumpStringHex(builder, version, "?");

        builder.append("  ");
        if (y.equals('q')) {
            builder.append("q:");
            BEValue q = message.dictionary.get("q");
            dumpString(builder, q, "?");

            dumpQueryArguments(builder, q, message.dictionary.get("a"));
        }
        else if (y.equals('r'))  {
            builder.append("r:");
            BEValue r = message.dictionary.get("r");
            dumpAsString(r, builder);
            dumpResponse(builder, r);
        }
        else {
            builder.append("?");
        }

        System.out.println(builder.toString());
    }

    private static void dumpQueryArguments(StringBuilder builder, BEValue query, BEValue a) {
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

    private static void dumpResponse(StringBuilder builder, BEValue response) {
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

    private static void dumpNodesCompactForm(StringBuilder builder, byte[] binary) {
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
