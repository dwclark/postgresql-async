package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class DataRow extends Response {

    private final int numberColumns;
    private final ByteBuffer buffer;
    
    public DataRow(final ByteBuffer buffer) {
        super(buffer);
        this.buffer = buffer;
        numberColumns = buffer.getShort() & 0xFFFF;
    }

    DataRowIterator iterator(final RowDescription description) {
        return new Iterator(description);
    }

    private class Iterator implements DataRowIterator {
        private int index = 0;
        private RowDescription description;

        private Iterator(final RowDescription description) {
            this.description = description;
        }

        public boolean hasNext() {
            return index < numberColumns;
        }

        private FieldDescriptor field() {
            return description.field(index++);
        }

        public Object next() {
            throw new UnsupportedOperationException();
        }

        public <T> T next(final Class<T> type) {
            throw new UnsupportedOperationException();
        }
        
        public boolean nextBoolean() {
            throw new UnsupportedOperationException();
        }
        
        public double nextDouble() {
            throw new UnsupportedOperationException();
        }
        
        public float nextFloat() {
            throw new UnsupportedOperationException();
        }
        
        public int nextInt() {
            throw new UnsupportedOperationException();
        }

        public long nextLong() {
            throw new UnsupportedOperationException();
        }
        
        public short nextShort() {
            throw new UnsupportedOperationException();
        }
    }
}
