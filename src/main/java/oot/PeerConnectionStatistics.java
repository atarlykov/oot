package oot;

/**
 * collects various statistics for a bound peer connection
 */
public class PeerConnectionStatistics {

    /**
     * collects incremental statistics
     */
    public static class Statistics {
        // time period in ms for one counter
        final long period;
        // counters' data with history
        final long[] data;
        // full time diapason of statistics,
        final long diapason;

        // currently active period
        int head;
        // last period
        int tail;
        // time mark of the head
        long timeHead;

        /**
         * constructor, initializes one active cell with the current time
         * @param _length number of history cells to store
         * @param _period period in ms for each history cell
         */
        public Statistics(int _length, long _period) {
            data = new long[_length];
            period = _period;
            diapason = _period * _length;
            reset();
        }

        /**
         * uses current time in ms to add value into history
         * @param value value to add to history
         */
        public void add(long value) {
            add(System.currentTimeMillis(), value);
        }

        /**
         * adds value to current cell if timestamp is inside it's period
         * or shifts history to make head cell ready for the specified timestamp
         * @param timestamp timestamp of the value
         *                  could point only to the current cell or new one,
         *                  adding to history is restricted
         * @param value value to add
         */
        private void add(long timestamp, long value) {
            if (timestamp < timeHead) {
                // adding to old cells in restricted
                return;
            }
            if (timeHead + period < timestamp) {
                grow(timestamp);
            }
            data[head] += value;
        }

        /**
         * allocates new cells to make head cell ready to store timestamped value
         * @param timestamp timestamp of a value to be ready to accept
         */
        private void grow(long timestamp)
        {
            if (diapason < timestamp - timeHead) {
                // clear all history,
                // set size = 1 (tail == head)
                head = tail = 0;
                data[head] = 0;

                long periods = (timestamp - timeHead) / period;
                timeHead += periods * period;
                return;
            }

            while (timeHead + period <= timestamp) {
                head += 1;
                head %= data.length;
                if (head == tail) {
                    tail += 1;
                    tail %= data.length;
                }
                data[head] = 0;
                timeHead += period;
            }
        }

        /**
         * reset statistics to the default state with the current time
         */
        public void reset() {
            head = tail = 0;
            data[head] = 0;
            timeHead = System.currentTimeMillis();
        }

        /**
         * just primitive implementation, doesn't check for overflow,
         * return average value for the last completed periods
         * @param periods number of periods to average
         * @return floored average value
         */
        public long average(int periods)
        {
            // make sure we have correct head pointer
            grow(System.currentTimeMillis());

            int i = head;
            long sum = 0;
            int count = 0;
            while ((i != tail) && (count < periods)) {
                i -= 1;
                if (i < 0) {
                    i = data.length - 1;
                }
                sum += data[i];
                count += 1;
            }
            return (count != 0) ? sum / count : 0;
        }

        /**
         * @return value from the current (growing) cell
         * of history records, value could grow over time
         * till the end of the current period
         */
        public long last() {
            grow(System.currentTimeMillis());
            return data[head];
        }
    }

    /**
     * download speed history
     */
    Statistics download;
    /**
     * upload speed history
     */
    Statistics upload;
    /**
     * number of blocks received with the connection
     */
    long blocksReceived;
    /**
     * number of blocks sent over the connection
     */
    long blocksSent;

    /**
     * counter for incorrect blocks,
     * connection could be dropped in such case
     * so could be not used
     */
    long blocksRequestedIncorrect;

    /**
     * constructor
     * @param _periods number of history periods for upload/download
     * @param _length length of each period in ms
     */
    public PeerConnectionStatistics(int _periods, int _length) {
        download = new Statistics(_periods, _length);
        upload = new Statistics(_periods, _length);
    }

    /**
     * reset all fields to start from scratch
     */
    public void reset() {
        download.reset();
        upload.reset();
        blocksRequestedIncorrect = 0;
        blocksReceived = 0;
        blocksSent = 0;
    }

}
