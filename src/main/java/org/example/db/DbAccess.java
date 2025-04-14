package org.example.db;

public sealed abstract class DbAccess {

    public abstract Method getMethod();

    public static final class PutAccess extends DbAccess {

        private final long key;
        private final String value;

        public PutAccess(long key, String value) {
            this.key = key;
            this.value = value;
        }

        public long getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        @Override
        public Method getMethod() {
            return Method.PUT;
        }
    }

    public static final class GetAccess extends DbAccess {

        private final long key;

        public GetAccess(long key) {
            this.key = key;
        }

        public long getKey() {
            return key;
        }

        @Override
        public Method getMethod() {
            return Method.GET;
        }
    }

    public static final class DeleteAccess extends DbAccess {
        private final long key;

        public DeleteAccess(long key) {
            this.key = key;
        }

        public long getKey() {
            return key;
        }

        @Override
        public Method getMethod() {
            return Method.DELETE;
        }
    }

    public static final class ScanAccess extends DbAccess {
        private final int fromPartition;
        private final int toPartition;

        public ScanAccess(int fromPartition, int toPartition) {
            this.fromPartition = fromPartition;
            this.toPartition = toPartition;
        }

        public int getFromPartition() {
            return fromPartition;
        }

        public int getToPartition() {
            return toPartition;
        }

        @Override
        public Method getMethod() {
            return Method.SCAN;
        }
    }
}
