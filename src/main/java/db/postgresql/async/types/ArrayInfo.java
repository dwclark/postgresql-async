package db.postgresql.async.types;

import db.postgresql.async.messages.Format;
import db.postgresql.async.pginfo.PgType;
import db.postgresql.async.serializers.*;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import static db.postgresql.async.serializers.SerializationContext.registry;

public class ArrayInfo {

    private static int numberDimensions(final Object ary) {
        final String name = ary.getClass().getName();
        return 1 + name.lastIndexOf('[');
    }

    private int[] dimensions() {
        final int[] ret = new int[numberDimensions(ary)];
        Object current = ary;
        ret[0] = Array.getLength(current);
        for(int i = 1; i < ret.length; ++i) {
            current = Array.get(current, 0);
            ret[i] = Array.getLength(current);
        }

        return ret;
    }

    private int[] initialIndexes() {
        return new int[dimensions.length];
    }

    private int numberElements() {
        int total = 1;
        for(int i = 0; i < dimensions.length; ++i) {
            total *= dimensions[i];
        }

        return total;
    }

    private void increment(final int[] indexes) {
        int indexToIncrement = 0;
        for(int i = indexes.length - 1; i >= 0; --i) {
            if(indexes[i] + 1 < dimensions[i]) {
                indexToIncrement = i;
                break;
            }
        }

        indexes[indexToIncrement] = 1 + indexes[indexToIncrement];
        for(int i = indexToIncrement + 1; i < indexes.length; ++i) {
            indexes[i] = 0;
        }
    }

    private Object lastAry(final int[] indexes) {
        Object ret = ary;
        for(int i = 0; i < indexes.length - 1; ++i) {
            ret = Array.get(ret, indexes[i]);
        }

        return ret;
    }
    
    private final PgType pgType;
    private final Object ary;
    private final int[] dimensions;
    private final int numberElements;
    private final Class elementType;
    
    public PgType getPgType() { return pgType; }
    public Object getAry() { return ary; }
    public int[] getDimensions() { return dimensions; }
    public int getNumberElements() { return numberElements; }
    
    private Class elementType() {
        final String n = lastAry(initialIndexes()).getClass().getName();
        final int lastIndex = n.lastIndexOf('[');
        final String id = n.substring(lastIndex, lastIndex + 2);
        switch(id) {
        case "[L": return Object.class;
        case "[Z": return boolean.class;
        case "[B": return byte.class;
        case "[S": return short.class;
        case "[I": return int.class;
        case "[J": return long.class;
        case "[F": return float.class;
        case "[D": return double.class;
        default: throw new UnsupportedOperationException("Can't handle serialization of array type: " + id);
        }
    }
    
    public ArrayInfo(final String name, final Object ary) {
        assert(ary.getClass().isArray());
        
        this.pgType = registry().pgType(name);
        this.ary = ary;
        this.dimensions = dimensions();
        this.numberElements = numberElements();
        this.elementType = elementType();
    }

    public ArrayInfo(final ByteBuffer buffer, final Class elementType) {
        this.dimensions = new int[buffer.getInt()];
        buffer.getInt(); //ignore offset information;
        final int oid = buffer.getInt();
        this.pgType = registry().pgType(oid);
        for(int i = 0; i < dimensions.length; ++i) {
            dimensions[i] = buffer.getInt();
            buffer.getInt(); //ignore lower bound information
        }

        this.numberElements = numberElements();
        this.elementType = elementType.isPrimitive() ? elementType : registry().serializer(pgType.getOid()).getType();
        this.ary = Array.newInstance(this.elementType, dimensions);

        read(buffer);
    }

    @FunctionalInterface
    private interface ArrayWriter {
        public void writeFromIndex(Object ary, int index);
    }

    @FunctionalInterface
    private interface ArrayReader {
        public void readFromIndex(Object ary, int index);
    }

    private static class ObjectWriter implements ArrayWriter {
        final Serializer s;
        final ByteBuffer buffer;
        
        public ObjectWriter(final Serializer s, final ByteBuffer buffer) {
            this.s = s;
            this.buffer = buffer;
        }

        @SuppressWarnings("unchecked")
        public void writeFromIndex(final Object ary, final int i) {
            s.write(buffer, Array.get(ary, i), Format.BINARY);
        }
    }

    private static class ObjectReader implements ArrayReader {
        final Serializer s;
        final ByteBuffer buffer;

        public ObjectReader(final Serializer s, final ByteBuffer buffer) {
            this.s = s;
            this.buffer = buffer;
        }

        @SuppressWarnings("unchecked")
        public void readFromIndex(final Object ary, final int i) {
            Array.set(ary, i, s.read(buffer, Format.BINARY));
        }
    }

    public void toBuffer(final ByteBuffer buffer) {
        buffer.putInt(dimensions.length);
        buffer.putInt(0); //we don't send arrays with offsets
        buffer.putInt(pgType.getOid());
        for(int i = 0; i < dimensions.length; ++i) {
            buffer.putInt(dimensions[i]);
            buffer.putInt(0); //lower bound is always zero
        }
              
        if(elementType == Object.class) {
            write(new ObjectWriter(registry().serializer(pgType.getOid()), buffer));
        }
        else if(elementType == boolean.class) {
            write((ary, i) -> BooleanSerializer.instance.writePrimitive(buffer, Array.getBoolean(ary, i), Format.BINARY));
        }
        else if(elementType == byte.class) {
            write((ary, i) -> {
                    buffer.putInt(1);
                    buffer.put(Array.getByte(ary, i));
                });
        }
        else if(elementType == short.class) {
            write((ary, i) -> ShortSerializer.instance.writePrimitive(buffer, Array.getShort(ary, i), Format.BINARY));
        }
        else if(elementType == int.class) {
            write((ary, i) -> IntegerSerializer.instance.writePrimitive(buffer, Array.getInt(ary, i), Format.BINARY));
        }
        else if(elementType == long.class) {
            write((ary, i) -> LongSerializer.instance.writePrimitive(buffer, Array.getLong(ary, i), Format.BINARY));
        }
        else if(elementType == float.class) {
            write((ary, i) -> FloatSerializer.instance.writePrimitive(buffer, Array.getFloat(ary, i), Format.BINARY));
        }
        else if(elementType == double.class) {
            write((ary, i) -> DoubleSerializer.instance.writePrimitive(buffer, Array.getDouble(ary, i), Format.BINARY));
        }
        else {
            throw new UnsupportedOperationException("Unsupported array type");
        }
    }

    private void write(final ArrayWriter writer) {
        final int[] indexes = initialIndexes();
        for(int i = 0; i < numberElements; ++i) {
            final Object last = lastAry(indexes);
            writer.writeFromIndex(last, indexes[indexes.length - 1]);
            increment(indexes);
        }
    }

    private void read(final ByteBuffer buffer) {
        if(!elementType.isPrimitive()) {
            read(new ObjectReader(registry().serializer(pgType.getOid()), buffer));
        }
        else if(elementType == boolean.class) {
            read((ary, i) -> Array.setBoolean(ary, i, BooleanSerializer.instance.readPrimitive(buffer, Format.BINARY)));
        }
        else if(elementType == byte.class) {
            read((ary, i) -> {
                    final int size = buffer.getInt();
                    if(size == -1) {
                        Array.setByte(ary, i, (byte) 0);
                    }
                    else {
                        Array.setByte(ary, i, buffer.get());
                    } });
        }
        else if(elementType == short.class) {
            read((ary, i) -> Array.setShort(ary, i, ShortSerializer.instance.readPrimitive(buffer, Format.BINARY)));
        }
        else if(elementType == int.class) {
            read((ary, i) -> Array.setInt(ary, i, IntegerSerializer.instance.readPrimitive(buffer, Format.BINARY)));
        }
        else if(elementType == long.class) {
            read((ary, i) -> Array.setLong(ary, i, LongSerializer.instance.readPrimitive(buffer, Format.BINARY)));
        }
        else if(elementType == float.class) {
            read((ary, i) -> Array.setFloat(ary, i, FloatSerializer.instance.readPrimitive(buffer, Format.BINARY)));
        }
        else if(elementType == double.class) {
            read((ary, i) -> Array.setDouble(ary, i, DoubleSerializer.instance.readPrimitive(buffer, Format.BINARY)));
        }
        else {
            throw new UnsupportedOperationException("Unsupported array type");
        }
    }

    private void read(final ArrayReader reader) {
        final int[] indexes = initialIndexes();
        for(int i = 0; i < numberElements; ++i) {
            final Object last = lastAry(indexes);
            reader.readFromIndex(last, indexes[indexes.length - 1]);
            increment(indexes);
        }
    }
}
