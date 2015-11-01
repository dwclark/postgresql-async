package db.postgresql.async.messages;

import db.postgresql.async.pginfo.Registry;
import db.postgresql.async.serializers.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class DataRow extends Response {

    private final int numberColumns;
    private final ByteBuffer buffer;

    private DataRow(final BackEnd backEnd, final int size, final int numberColumns, ByteBuffer buffer) {
        super(backEnd, size);
        this.numberColumns = numberColumns;
        this.buffer = buffer;
    }
    
    public DataRow(final ByteBuffer buffer) {
        super(buffer);
        this.buffer = buffer;
        numberColumns = buffer.getShort() & 0xFFFF;
    }

    public DataRow detach() {
        final ByteBuffer copied = ByteBuffer.allocate(getSize());
        final int limit = buffer.limit();
        buffer.limit(buffer.position() + getSize());
        copied.put(buffer);
        buffer.limit(limit);
        copied.flip();
        return new DataRow(BackEnd.DataRow, getSize(), numberColumns, copied);
    }

    public void skip() {
        buffer.position(buffer.position() + getSize());
    }

    public List<Object> toList(final RowDescription description, final Registry registry) {
        List<Object> ret = new ArrayList<>(numberColumns);
        DataRowIterator iter = iterator(description, registry);
        while(iter.hasNext()) {
            ret.add(iter.next());
        }

        return ret;
    }

    public Map<String,Object> toMap(final RowDescription description, final Registry registry) {
        Map<String,Object> ret = new LinkedHashMap<>(numberColumns);
        Iterator iter = new Iterator(description, registry);
        while(iter.hasNext()) {
            ret.put(iter.nextField().getName(), iter.next());
        }

        return ret;
    }
    
    DataRowIterator iterator(final RowDescription description, final Registry registry) {
        return new Iterator(description, registry);
    }

    private class Iterator implements DataRowIterator {
        private int index = -1;
        private FieldDescriptor field;
        private final RowDescription description;
        private final Registry registry;

        private Iterator(final RowDescription description, final Registry registry) {
            this.description = description;
            this.registry = registry;
        }

        private void advance() {
            ++index;
            field = description.field(index);
            if(index >= numberColumns) {
                throw new NoSuchElementException();
            }
        }
        
        public boolean hasNext() {
            return index < numberColumns;
        }
        
        public FieldDescriptor nextField() {
            if(index >= numberColumns) {
                throw new NoSuchElementException();
            }
            
            return description.field(index + 1);
        }

        public Object next() {
            advance();
            return registry.serializer(field.getTypeOid()).read(buffer, buffer.getInt());
        }

        public <T> T next(final Class<T> type) {
            advance();
            return registry.serializer(type).read(buffer, buffer.getInt());
        }
        
        public boolean nextBoolean() {
            advance();
            return BooleanSerializer.instance.readPrimitive(buffer, buffer.getInt());
        }
        
        public double nextDouble() {
            advance();
            return DoubleSerializer.instance.readPrimitive(buffer, buffer.getInt());
        }
        
        public float nextFloat() {
            advance();
            return FloatSerializer.instance.readPrimitive(buffer, buffer.getInt());
        }
        
        public int nextInt() {
            advance();
            return IntegerSerializer.instance.readPrimitive(buffer, buffer.getInt());
        }

        public long nextLong() {
            advance();
            return LongSerializer.instance.readPrimitive(buffer, buffer.getInt());
        }
        
        public short nextShort() {
            advance();
            return ShortSerializer.instance.readPrimitive(buffer, buffer.getInt());
        }
    }    
}
