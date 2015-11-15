package db.postgresql.async.messages;

import db.postgresql.async.Row;
import db.postgresql.async.pginfo.Registry;
import db.postgresql.async.serializers.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class DataRow extends Response implements Row {

    private final RowDescription description;
    private final Registry registry;
    private final ByteBuffer buffer;
    private final int base;
    
    private DataRow(final BackEnd backEnd, final int size, final ByteBuffer buffer,
                    final RowDescription description, final Registry registry) {
        super(backEnd, size);
        this.buffer = buffer;
        buffer.position(buffer.position() + 2); //skip column count
        this.description = description;
        this.registry = registry;
        this.base = 0;
    }
    
    public DataRow(final ByteBuffer buffer) {
        super(buffer);
        this.buffer = buffer;
        this.base = buffer.position();
        buffer.position(buffer.position() + 2); //skip column count
        this.description = SerializationContext.description();
        this.registry = SerializationContext.registry();
    }

    public void finish() {
        buffer.position(base + getSize());
    }

    public DataRow detach() {
        final ByteBuffer copied = ByteBuffer.allocate(getSize());
        final int limit = buffer.limit();
        final int position = buffer.position();
        buffer.position(base);
        buffer.limit(base + getSize());
        copied.put(buffer);
        buffer.position(position);
        buffer.limit(limit);
        copied.flip();
        return new DataRow(BackEnd.DataRow, getSize(), buffer, description, registry);
    }

    public void skip() {
        buffer.position(buffer.position() + getSize());
    }

    public Iterator iterator() {
        return new Iterator();
    }

    public Extractor extractor() {
        return new Extractor();
    }

    public int length() {
        return description.length();
    }

    public String name(final int index) {
        return description.field(index).getName();
    }

    public String[] getNames() {
        return description.getNames();
    }
    
    private class Iterator implements Row.Iterator {
        private int index = 0;
        private FieldDescriptor field;

        private void advance() {
            ++index;
            field = description.field(index-1);
            if(index > description.length()) {
                throw new NoSuchElementException();
            }
        }

        public int length() {
            return description.length();
        }

        public boolean hasNext() {
            return index < description.length();
        }
        
        public Object next() {
            advance();
            return registry.serializer(field.getTypeOid()).read(buffer, buffer.getInt());
        }

        public <T> T next(final Class<T> type) {
            advance();
            return registry.serializer(type).read(buffer, buffer.getInt());
        }

        public String nextString() {
            advance();
            return StringSerializer.instance.read(buffer, buffer.getInt());
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

    private class Extractor implements Row.Extractor {
        private final RowDescription description = SerializationContext.description();
        private final Registry registry = SerializationContext.registry();

        private void place(final int index) {
            if(index >= description.length()) {
                throw new ArrayIndexOutOfBoundsException();
            }

            buffer.mark();
            for(int i = 0; i < index; ++i) {
                final int size = buffer.getInt();
                buffer.position(buffer.position() + Math.max(size, 0));
            }
        }

        public int length() {
            return description.length();
        }

        public Object getAt(final String field) {
            return getAt(description.indexOf(field));
        }
        
        public Object getAt(final int index) {
            try {
                place(index);
                return registry.serializer(description.field(index).getTypeOid()).read(buffer, buffer.getInt());
            }
            finally {
                buffer.reset();
            }
        }
        
        public <T> T objectAt(final Class<T> type, final String field) {
            return objectAt(type, description.indexOf(field));
        }
        
        public <T> T objectAt(final Class<T> type, final int index) {
            try {
                place(index);
                return registry.serializer(type).read(buffer, buffer.getInt());
            }
            finally {
                buffer.reset();
            }
        }
        
        public boolean booleanAt(final String field) {
            return booleanAt(description.indexOf(field));
        }
        
        public boolean booleanAt(final int index) {
            try {
                place(index);
                return BooleanSerializer.instance.readPrimitive(buffer, buffer.getInt());
            }
            finally {
                buffer.reset();
            }
        }
        
        public double doubleAt(final String field) {
            return doubleAt(description.indexOf(field));
        }
        
        public double doubleAt(final int index) {
            try {
                place(index);
                return DoubleSerializer.instance.readPrimitive(buffer, buffer.getInt());
            }
            finally {
                buffer.reset();
            }
        }
        
        public float floatAt(final String field) {
            return floatAt(description.indexOf(field));
        }
        
        public float floatAt(final int index) {
            try {
                place(index);
                return FloatSerializer.instance.readPrimitive(buffer, buffer.getInt());
            }
            finally {
                buffer.reset();
            }
        }
        
        public int intAt(final String field) {
            return intAt(description.indexOf(field));
        }
        
        public int intAt(final int index) {
            try {
                place(index);
                return IntegerSerializer.instance.readPrimitive(buffer, buffer.getInt());
            }
            finally {
                buffer.reset();
            }
        }
        
        public long longAt(final String field) {
            return longAt(description.indexOf(field));
        }
        
        public long longAt(final int index) {
            try {
                place(index);
                return LongSerializer.instance.readPrimitive(buffer, buffer.getInt());
            }
            finally {
                buffer.reset();
            }
        }
        
        public short shortAt(final String field) {
            return shortAt(description.indexOf(field));
        }
        
        public short shortAt(final int index) {
            try {
                place(index);
                return ShortSerializer.instance.readPrimitive(buffer, buffer.getInt());
            }
            finally {
                buffer.reset();
            }
        }
    }
}
