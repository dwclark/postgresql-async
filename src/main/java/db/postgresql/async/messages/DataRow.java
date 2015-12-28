package db.postgresql.async.messages;

import db.postgresql.async.Row;
import db.postgresql.async.pginfo.Registry;
import db.postgresql.async.pginfo.PgType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import static db.postgresql.async.serializers.Primitives.*;
import db.postgresql.async.serializers.SerializationContext;

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

    private Object extractByPgType(final int index) {
        final FieldDescriptor field = description.field(index);
        final int oid = field.getTypeOid();
        final PgType pgType = registry.pgType(oid);
        if(pgType != null) {
            return pgType.read(buffer, oid);
        }
        else {
            throw new UnsupportedOperationException("Can't deserialize type with oid " + oid);
        }
    }

    private class Iterator implements Row.Iterator {
        private int index = 0;
        private FieldDescriptor field;
        
        private int advance() {
            if(index == description.length()) {
                throw new NoSuchElementException();
            }

            field = description.field(index);
            return index++;
        }

        public int length() {
            return description.length();
        }

        public boolean hasNext() {
            return index < description.length();
        }
        
        public Object next() {
            return extractByPgType(advance());
        }

        //handle separately to allow bootstrap to use this
        public String nextString() {
            final int size = buffer.getInt();
            if(size == -1) {
                return null;
            }
            else {
                return SerializationContext.bufferToString(size, buffer);
            }
        }

        public boolean nextBoolean() {
            advance();
            return readBoolean(buffer);
        }
        
        public double nextDouble() {
            advance();
            return readDouble(buffer);
        }
        
        public float nextFloat() {
            advance();
            return readFloat(buffer);
        }
        
        public int nextInt() {
            advance();
            return readInt(buffer);
        }

        public long nextLong() {
            advance();
            return readLong(buffer);
        }
        
        public short nextShort() {
            advance();
            return readShort(buffer);
        }

        public Object nextArray(final Class elementType) {
            advance();
            final int oid = field.getTypeOid();
            final PgType pgType = registry.pgType(oid);
            return pgType.read(buffer, field.getTypeOid(), elementType);
        }
    }

    private class Extractor implements Row.Extractor {
        
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
                return extractByPgType(index);
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
                return readBoolean(buffer);
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
                return readDouble(buffer);
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
                return readFloat(buffer);
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
                return readInt(buffer);
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
                return readLong(buffer);
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
                return readShort(buffer);
            }
            finally {
                buffer.reset();
            }
        }

        public Object arrayAt(final String field, final Class elementType) {
            return arrayAt(description.indexOf(field), elementType);
        }

        public Object arrayAt(final int index, final Class elementType) {
            place(index);
            final FieldDescriptor field = description.field(index);
            final int oid = field.getTypeOid();
            final PgType pgType = registry.pgType(oid);
            return pgType.read(buffer, field.getTypeOid(), elementType);
        }
    }
}
