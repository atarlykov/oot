package oot;

public class PeerStatistics {

    public static final int PERIODS = 20;
    public static final int PERIOD = 1000;

    /**
     * collects incremental statistics
     */
    public static class Statistics {
        // time period in ms for one counter
        final long period;
        // counters' data with history
        long[] data;
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
         * adds value to current cell if timestamps is the same
         * or creates new ones to make new current with the specified timestamp
         * @param timestamp base timestamp to fund cell to add value too,
         *                  could point only to the current cell or new one,
         *                  adding to history is restricted
         * @param value value to add
         */
        private void add(long timestamp, long value) {
            long p = timestamp / period;
            if (p != timeHead) {
                if (p < timeHead) {
                    // adding to old cells in restricted
                    return;
                }
                grow(p - timeHead);
            }
            data[head] += value;
        }

        /**
         * allocates new cells to allow more periods in the history
         * @param periods number of cells to add
         */
        private void grow(long periods) {

            if (data.length < periods) {
                // clear all history,
                // set size = 1 (tail == head)
                head = tail = 0;
                data[head] = 0;
                timeHead += periods;
                return;
            }

            // move pointers, allow growing
            // if not maximized yet
            for (int i = 0; i < periods; i++) {
                head += 1;
                head %= data.length;
                if (head == tail) {
                    tail += 1;
                    tail %= data.length;
                }
                data[head] = 0;
            }

            timeHead += periods;
        }

        /**
         * reset statistics to the default state with the current time
         */
        public void reset() {
            head = tail = 0;
            data[head] = 0;
            timeHead = System.currentTimeMillis() / period;
        }

        /**
         * just primitive implementation
         * @param periods number of periods to avarage
         * @return floored average value
         */
        public long average(int periods) {
            int i = head;

            long sum = data[head];
            int count = 1;
            while ((i != tail) && (count < periods)) {
                i -= 1;
                if (i < 0) {
                    i = data.length - 1;
                }
                sum += data[i];
                count += 1;
            }
            return sum / count;
        }

        public long average() {
            return average(PERIODS);
        }
    }


    Statistics download = new Statistics(PERIODS, PERIOD);
    Statistics upload = new Statistics(PERIODS, PERIOD);


    long blocksReceived = 0;
    long blocksSent = 0;

    public PeerStatistics() {
    }

    public void reset() {
        download.reset();
        upload.reset();
    }

    public void received(long bytes) {
        received(System.currentTimeMillis(), bytes);
    }

    public void received(long timestamp, long bytes) {
        download.add(timestamp, bytes);
    }

    public void sent(long bytes) {
        sent(System.currentTimeMillis(), bytes);
    }

    public void sent(long timestamp, long bytes) {
        upload.add(timestamp, bytes);
    }

    public long downloadSpeed() {
        return download.average() * 1000 / PERIOD;
    }
    public long uploadSpeed() {
        return upload.average() * 1000 / PERIOD;
    }
}
