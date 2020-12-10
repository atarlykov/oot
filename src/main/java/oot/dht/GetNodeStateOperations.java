package oot.dht;

import java.util.function.Consumer;

/**
 * artificial operation to get state of the node (including routing table) from
 * the main thread without any synchronization inside node's code
 */
class GetNodeStateOperations extends Operation
{
    // callback to be called with state data
    private final Consumer<byte[]> callback;

    /**
     * allowed constructor
     * @param _node ref to local node
     * @param _callback callback to call with state data
     */
    public GetNodeStateOperations(Node _node, Consumer<byte[]> _callback) {
        super(_node);
        callback = _callback;
    }

    @Override
    boolean update() {
        // this must be done in update() as we need
        // synchronization while accessing state
        if (callback != null) {
            byte[] state = node.getState();
            callback.accept(state);
        }
        return OPERATION_FINISH;
    }
}
