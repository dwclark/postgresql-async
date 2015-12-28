package db.postgresql.async.pginfo;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import static db.postgresql.async.serializers.SerializationContext.registry;
import static db.postgresql.async.serializers.Primitives.*;

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
        case "[S": return short.class;
        case "[I": return int.class;
        case "[J": return long.class;
        case "[F": return float.class;
        case "[D": return double.class;
        default: throw new UnsupportedOperationException("Can't handle serialization of array type: " + id);
        }
    }
    
    public ArrayInfo(final PgType pgType, final Object ary) {
        this.pgType = pgType;
        this.ary = ary;
        this.dimensions = dimensions();
        this.numberElements = numberElements();
        this.elementType = elementType();
    }

    public ArrayInfo(final PgType pgType, final ByteBuffer buffer, final Class elementType) {
        this.pgType = pgType;
        this.elementType = elementType;
        this.dimensions = new int[buffer.getInt()];
        buffer.getInt(); //ignore offset information;
        final int oid = buffer.getInt(); //ignore oid information
        for(int i = 0; i < dimensions.length; ++i) {
            dimensions[i] = buffer.getInt();
            buffer.getInt(); //ignore lower bound information
        }

        this.numberElements = numberElements();
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

    public void toBuffer(final ByteBuffer buffer) {
        buffer.putInt(dimensions.length);
        buffer.putInt(0); //we don't send arrays with offsets
        buffer.putInt(pgType.getOid());
        for(int i = 0; i < dimensions.length; ++i) {
            buffer.putInt(dimensions[i]);
            buffer.putInt(0); //lower bound is always zero
        }
              
        if(elementType == boolean.class) {
            write((ary, i) -> writeBoolean(buffer, Array.getBoolean(ary, i)));
        }
        else if(elementType == short.class) {
            write((ary, i) -> writeShort(buffer, Array.getShort(ary, i)));
        }
        else if(elementType == int.class) {
            write((ary, i) -> writeInt(buffer, Array.getInt(ary, i)));
        }
        else if(elementType == long.class) {
            write((ary, i) -> writeLong(buffer, Array.getLong(ary, i)));
        }
        else if(elementType == float.class) {
            write((ary, i) -> writeFloat(buffer, Array.getFloat(ary, i)));
        }
        else if(elementType == double.class) {
            write((ary, i) -> writeDouble(buffer, Array.getDouble(ary, i)));
        }
        else {
            write((ary, i) -> pgType.write(buffer, Array.get(ary, i)));
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
        if(elementType == boolean.class) {
            read((ary, i) -> Array.setBoolean(ary, i, readBoolean(buffer)));
        }
        else if(elementType == short.class) {
            read((ary, i) -> Array.setShort(ary, i, readShort(buffer)));
        }
        else if(elementType == int.class) {
            read((ary, i) -> Array.setInt(ary, i, readInt(buffer)));
        }
        else if(elementType == long.class) {
            read((ary, i) -> Array.setLong(ary, i, readLong(buffer)));
        }
        else if(elementType == float.class) {
            read((ary, i) -> Array.setFloat(ary, i, readFloat(buffer)));
        }
        else if(elementType == double.class) {
            read((ary, i) -> Array.setDouble(ary, i, readDouble(buffer)));
        }
        else {
            read((ary, i) -> Array.set(ary, i, pgType.read(buffer, pgType.getOid())));
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
