package oot.dht;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Consumer;

/**
 * Complex operation that runs bootstrapping procedure
 * on local node startup
 * cases:
 * 1. fresh start that needs neighbours search
 * 2. reload that must only publish itself (search new neighbours too??)
 */
class BootstrapOperation extends FindCloseNodesOperation {
    /**
     * max number of search iterations
     */
    private static final int MAX_ITERATIONS = 100;
    /**
     * default timeout for bootstrap operation to stop, in milliseconds
     */
    static final long OPERATION_TIMEOUT = 64_000L;

    /**
     * allowed constructor
     * operation will start from the seed collection,
     * nodes from the seed collection are will not be added to the routing table
     * @param _node ref to local node
     * @param _seed collection to start bootstrapping from
     * @param _extCallback callback to be called on completion with true parameter
     *                     if routing has active nodes in it and false otherwise
     */
    public BootstrapOperation(Node _node, List<InetSocketAddress> _seed, Consumer<Boolean> _extCallback) {
        super(_node, _node.id, (alive) ->
        {
            // will be called via Node's thread
            if (alive) {
                // notify callback if it's the 1st bootstrap
                Consumer<Void> cbBootstrapped = _node.callbackBootstrapped;
                if (!_node.bootstrapped && (cbBootstrapped != null)) {
                    cbBootstrapped.accept(null);
                }
                // update state
                _node.bootstrapped = true;
            }
            if (_extCallback != null) {
                _extCallback.accept(alive);
            }
        }, OPERATION_TIMEOUT, MAX_ITERATIONS, _seed);
    }
}
