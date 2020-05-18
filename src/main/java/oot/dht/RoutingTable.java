package oot.dht;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * based on distances to an anchor node to make
 * buckets' tree simpler
 */
public class RoutingTable {

    /**
     * Contains data about any remove DHT node stored in this routing table
     */
    public class RemoteNode {

        // timeout before sending a ping request to a node,
        // this is 15 minutes based on the spec
        static final long NODE_PING_TIMEOUT  = 15 * 60 * 1000;

        // timeout to consider a node as dead one and remove from the routing,
        // also node cou be removed earlier if it doesn't
        // reply several times
        static final long NODE_DEAD_TIMEOUT  = 30 * 60 * 1000;

        // number of missing replies + 1 from a node
        // to consider it ad dead
        static final long NODE_DEAD_REPLIES  = 2;

        // length of the local token generated
        // for communication with this node
        static final int TOKEN_LOCAL_LENGTH  = 8;

        // timeout for local tokens
        static final long TOKEN_LOCAL_TIMEOUT = 24*60*60*1000;
        // timeout for received tokens
        //static final long TOKEN_EXTERNAL_TIMEOUT = 24*60*60*1000;

        // timeout for node to be considered as downloader
        // after the last announce received
        static final long NODE_ANNOUNCE_TIMEOUT = 60*60*1000;

        // unique fixed node id
        final HashId id;

        // distance between this node and the local one,
        // routing table stores remote nodes based on this value,
        // not the original id
        final HashId distance;

        // address of the node, this
        // is used for DHT communications
        InetSocketAddress address;

        // port used by the remote peer to accept
        // incoming peer-2-peer connections,
        // zero if DHT port from #address must be used
        int peerPort;

        // token returned by find_peers query to this remote node,
        // will be sent with announce messages to this node
        byte[] token;

        // generated token for get_peers messages to this node,
        // that's enough to have only 1 token for all requests from the same ip,
        // just protection from some node to sign more nodes (ip addresses) with the same token
        private byte[] tokenLocal;

        // timestamp of the last activity - creation time, query or reply received
        long timeLastActivity;

        // timestamp of the last local token generated
        long timeTokenLocal;

        // timestamp of the last token received from this node (could be removed later)
        long timeToken;

        // timestamp of an external announce message from this node
        long timeAnnounceExtenal;

        // ??
        int deadReplies = 0;

        /**
         * allowed constructor
         * @param _id ndoe id
         * @param _address remote address
         */
        RemoteNode(HashId _id, InetSocketAddress _address) {
            id = _id;
            address = _address;
            distance = node.id.distance(id);
            timeLastActivity = System.currentTimeMillis();
        }

        /**
         * @return true is this node iss still considered as alive
         */
        boolean isAlive() {
            return (deadReplies < NODE_DEAD_REPLIES)
                    && (System.currentTimeMillis() < timeLastActivity + NODE_DEAD_TIMEOUT);
        }

        /**
         * @return true if ping timeout is reached
         */
        boolean isReadyForPing() {
            return timeLastActivity + NODE_PING_TIMEOUT < System.currentTimeMillis();
        }

        /**
         * @return true if node could be considered as dead one
         */
        boolean isDead() {
            return timeLastActivity + NODE_DEAD_TIMEOUT < System.currentTimeMillis();
        }

        /**
         * @return true if local token has expired
         */
        boolean isTokenLocalExpired() {
            return timeTokenLocal + TOKEN_LOCAL_TIMEOUT < System.currentTimeMillis();
        }

        /**
         * @return true if announce received from the node has expired
         */
        boolean isAnnounceExternalExpired() {
            return timeAnnounceExtenal + NODE_ANNOUNCE_TIMEOUT < System.currentTimeMillis();
        }

        /**
         * @return existing or newly generated token
         */
        public byte[] getTokenLocal() {
            if (tokenLocal == null) {
                tokenLocal = new byte[TOKEN_LOCAL_LENGTH];
                new Random().nextBytes(tokenLocal);
            }
            return tokenLocal;
        }

        /**
         * sets peer port of this node
         * @param _port port
         */
        public void setPeerPort(int _port) {
            peerPort = _port;
        }

        /**
         * updates last activity time of the node,
         * current time in milliseconds is used
         */
        void updateLastActivityTime() {
            updateLastActivityTime(System.currentTimeMillis());
        }

        /**
         * updates last activity time of the node
         * @param _timestamp last known activity time
         */
        void updateLastActivityTime(long _timestamp) {
            timeLastActivity = _timestamp;
            deadReplies = 0;
        }

        /**
         * method to be used for distance based orderings
         * @param node node to compare against
         * @return distance fields ordering based on {@link Comparator#compare}
         */
        int compareByDistance(RemoteNode node) {
            return this.distance.compare(node.distance);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RemoteNode that = (RemoteNode) o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
        }
    }

    /**
     * Bucket to organize nodes on the same level of routing tree
     */
    static class Bucket {
        /**
         * max allowed number of nodes on the same level
         * before level will be subdivided
         */
        static final int K = 8;
        /**
         * level of the bucket, top one is zero
         */
        private int level;

        /**
         * elements of the bucket, size is limited to {@link Bucket#K},
         * elements are sorted based on distance from the local node,
         * closest are first
         */
        List<RemoteNode> elements = new ArrayList<>(K);
        /**
         * ref to child bucket with nodes that
         * are more close to reference (local) node
         */
        Bucket child;
        /**
         * ref to parent bucket with nore distant nodes
         */
        Bucket parent;

        //private long timeout;

        /**
         * build root node
         */
        Bucket() {
            this(null);
        }

        /**
         * builds child node that will be linked to the specified parent
         * @param _parent ref to parent node, could be null for root node
         */
        Bucket(Bucket _parent) {
            parent = _parent;
            level = (_parent != null) ? _parent.level + 1 : 0;
        }

        /**
         * inserts new node into the table if there is a space for it
         * on the appropriate level
         * @param node node to insert
         * @return inserted node specified as parameter or the one
         * with the same id that is already in the tree,
         * returns NULL if node was not added due to space limits
         */
        RemoteNode insert(RemoteNode node) {
            // is this id could be moved to lower levels if possible
            boolean isIdForLowerLevel = !node.distance.getBit(level);

            if ((child != null) && isIdForLowerLevel) {
                // child tree already exists and 'id' is for lower levels
                return child.insert(node);
            }

            // try populate our level
            if (elements.size() < K) {
                // add node into the current bucket controlling sort order
                // (note: could return false if the same id is already in the list)
                return insertIntoBucketList(node);
            }

            // we don't try to remove dead nodes from the bucket here
            // as this is performed periodically during a ping procedure,
            // todo: remove
            // just try to subdivide the bucket and grow routing tree down.
            // note: it's possible that bucket already contains elements for child level,
            // so we must not depend on isIdForLowerLevel
            if ((child == null) && (level < HashId.HASH_LENGTH_BITS - 1)) {

                // potential child elements could only be stored at the beginning of the list,
                // as they all have zero left bit set,
                // count candidates for the next lower level
                int index = 0;
                while ((index < elements.size()) && !elements.get(index).distance.getBit(level)) {
                    index++;
                }

                // now index points to the 1st element than can't be moved to the lower level

                if (isIdForLowerLevel || (0 < index)) {
                    // we have nodes in the bucket that could be moved to the next level
                    // or new node is itself is for the next level,
                    // so we need the next level
                    child = new Bucket(this);

                    // elements are ordered already,
                    // so just copy them saving order
                    for (int i = 0; i < index; i++) {
                        child.elements.add( elements.get(i));
                    }

                    // batch clear copied elements
                    elements.subList(0, index).clear();

                    if (isIdForLowerLevel) {
                        // insert with sort
                        return child.insert(node);
                    } else {
                        return insertIntoBucketList(node);
                    }
                }
            }

            return null;
        }


        /**
         * inserts node into the bucket, controlling it's max size and
         * status of existing nodes
         * @param node node to insert
         * @return inserted node or the one that is already in the bucket
         */
        private RemoteNode insertIntoBucketList(RemoteNode node) {
            // add node into the current bucket controlling sort order
            int index = Collections.binarySearch(elements, node, RemoteNode::compareByDistance);
            if (index < 0) {
                elements.add( -(index + 1), node);
                return node;
            } else {
                // node is already in the bucket
                return elements.get(index);
            }
        }

        /**
         * removes dead {@link RemoteNode#isDead()} nodes from the bucket
         * @return number of removed nodes
         */
        int removeDead() {
            int counter = 0;
            for (int i = elements.size() - 1; 0 <= i; i--) {
                if (elements.get(i).isDead()) {
                    elements.remove(i);
                    counter++;
                }
            }
            return counter;
        }
    }


    // root point of the routing table,
    // top level stores most distant peers
    Bucket root = new Bucket();

    // ref to the local node that operates
    // this routing table
    Node node;

    /**
     * Builds new routing table that must be bootstrapped/populated
     * from the local node
     * @param node local dht node
     */
    public RoutingTable(Node node) {
        this.node = node;
    }

    /**
     * inserts another known DHT node into this routing table,
     * could be used for initial bootstrapping
     * @param node node to insert
     * @return inserted node specified as parameter or the one
     * with the same id that is already in the tree,
     * returns NULL if node was not added due to space limits
     * @see RoutingTable#insert(RemoteNode)
     */
    public RemoteNode insert(RemoteNode node) {
        return root.insert(node);
    }

    /**
     * inserts another known DHT node into this routing table
     * @param id id of the node to insert
     * @param address address of the node
     * @return inserted node specified as parameter or the one
     * with the same id that is already in the tree,
     * returns NULL if node was not added due to space limits
     * @see RoutingTable#insert(RemoteNode)
     */
    public RemoteNode insert(HashId id, InetSocketAddress address) {
        return insert(new RemoteNode(id, address));
    }

    /**
     * search routing table for the specified node and updates it if found,
     * updates lastAccessTime and token (if present in the specified node),
     * if node is not found, tries to insert into the routing table
     * @param node node to be inserted or updated
     */
    public void update(RemoteNode node) {
        RemoteNode nodeInTable = root.insert(node);
        if ((nodeInTable != null) && (nodeInTable != node)) {
            nodeInTable.updateLastActivityTime();
            if (node.token != null) {
                nodeInTable.token = node.token;
                nodeInTable.timeToken = System.currentTimeMillis();
            }
        }
    }

    /**
     * @return ref to the lowest bucket that contains nearest dht nodes
     */
    private Bucket getNearestBucket() {
        Bucket current = root;
        while (current.child != null) {
            current = current.child;
        }
        return current;
    }

    /**
     * searches the routing table for closest nodes to the given target hash
     * @param target hash id to find closes nodes to
     * @return not null list of nodes available in the routing table, size is limited to {@link Bucket#K} elements
     */
    List<RoutingTable.RemoteNode> findClosestNodes(HashId target) {
        List<RemoteNode> nodes = new ArrayList<>(Bucket.K);
        HashId[] distances = new HashId[Bucket.K];
        Bucket bucket = root;
        do {
            for (RemoteNode node: bucket.elements) {
                populateClosestList(target, nodes, distances, node);
            }
            bucket = bucket.child;
        } while (bucket != null);
        return nodes;
    }

    /**
     * checks routing table to contain at least one alive node
     * @return true if there is at least one alive node and false otherwise
     */
    boolean hasAliveNodes() {
        Bucket current = root;
        do {
            for (RemoteNode rNode: current.elements) {
                if (rNode.isAlive()) {
                    return true;
                }
            }
            current = current.child;
        } while (current != null);

        return false;
    }

    /**
     * utility method to build collection of nodes, closest to the specified target hash
     * @param target target hash to calculate distances against
     * @param nodes result collection of nodes with the fixed max size {@link Bucket#K}
     * @param distances utility array of the size {@link Bucket#K} with distance for elements in nodes list
     * @param node another node to add into the closest collection
     */
    void populateClosestList(HashId target, List<RemoteNode> nodes, HashId[] distances, RemoteNode node) {
        // make alias
        int nodesSize = nodes.size();

        // find distance between new node and target hash and find place for it in the array
        HashId distance = node.id.distance(target);
        int index = Arrays.binarySearch(distances, 0, nodesSize, distance, HashId::compare);

        if (0 <= index) {
            // ok, we already have node with the same distance,
            // check if it's the same or not
            if (nodes.get(index).id.compare(node.id) == 0) {
                // exactly this node already exists
                return;
            }
            // it's possible that there are more nodes with the same distance,
            // but possibility is too low, so just let it be,
            // could be filtered out at the end
        } else {
            // index to insert
            index = -(index + 1);
        }

        // check if max allowed number of closest nodes is still not reached,
        // simply insert new node into the place
        if (nodesSize < distances.length) {
            System.arraycopy(distances, index, distances, index + 1, nodesSize - index);
            distances[index] = distance;
            nodes.add(index, node);
            return;
        }

        if (index == distances.length) {
            // seems new node is too far away
        } else {
            // max allowed number is reached, throw out end element
            System.arraycopy(distances, index, distances, index + 1, distances.length - index - 1);
            distances[index] = distance;
            nodes.remove(nodesSize - 1);
            nodes.add(index, node);
        }
    }


    /**
     * searches routing table for node with the specified id
     * @param id id of the node to search for
     * @return node if found in the routing table or null
     */
    RemoteNode getRemoteNode(HashId id) {
        int level = 0;

        // calculate distance for the id to speed up tree navigation
        HashId distance = node.getId().distance(id);

        Bucket current = root;
        do {
            // is this id for current level or lower one
            boolean isIdForLowerLevel = !distance.getBit(level);

            if (!isIdForLowerLevel || (current.child == null)) {
                // distance must be stored on this level
                // or lower one but it's still missing,
                // so this level is the last to search
                for (RemoteNode rNode: current.elements) {
                    int compare = rNode.distance.compare(distance);
                    if (0 < compare) {
                        // tail of the list contains elements
                        // with greater distance only
                        return null;
                    }
                    if ((compare == 0) && (rNode.id.equals(id))) {
                        // distance and id are the same
                        return rNode;
                    }
                }
                // no node on our level
                return null;
            }
            // go to the lower level
            level++;
            current = current.child;
        } while (true);
    }

    /**
     * @return total number of nodes inside the table
     */
    public int count() {
        int counter = 0;
        Bucket current = root;
        do {
            counter += current.elements.size();
            current = current.child;
        } while (current != null);
        return counter;
    }

    /**
     * @return number of levels inside the table
     */
    public int levels() {
        int levels = 0;
        Bucket current = root;
        do {
            current = current.child;
            levels++;
        } while (current != null);
        return levels;
    }

    /**
     * @return list of all remote nodes from the routing table
     */
    public List<RemoteNode> getRemoteNodes() {
        List<RemoteNode> tmp = new ArrayList<>();
        Bucket current = root;
        do {
            tmp.addAll(current.elements);
            current = current.child;
        } while (current != null);
        return tmp;
    }
}
