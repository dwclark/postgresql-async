package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import static db.postgresql.async.serializers.SerializationContext.*;

public class BitSetSerializer extends Serializer<BitSet> {

    public static final BitSetSerializer instance = new BitSetSerializer();

    public List<String> getPgNames() {
        return Arrays.asList("pg_catalog.bit", "pg_catalog.varbit");
    }
    
    public Class<BitSet> getType() { return BitSet.class; }
    
    public BitSet fromString(final String str) {
        final int size = str.length();
        BitSet ret = new BitSet(size);
        for(int i = 0; i < size; ++i) {
            ret.set(i, str.charAt(i) == '1' ? true : false);
        }
        
        return ret;
    }

    public String toString(final BitSet bits) {
        if(bits == null) {
            return null;
        }

        final CharBuffer buffer = stringOps().ensure(bits.length());
        for(int i = 0; i < bits.length(); ++i) {
            buffer.put(bits.get(i) ? '1' : '0');
        }

        return buffer.flip().toString();
    }
    
    public BitSet read(final ByteBuffer buffer, final int size) {
        if(isNull(size)) {
            return null;
        }

        BitSet ret = new BitSet(size);
        for(int i = 0; i < size; ++i) {
            final byte b = buffer.get();
            final boolean val = (b == (byte) '1') ? true : false;
            ret.set(i, val);
        }

        return ret;
    }

    public void write(final ByteBuffer buffer, final BitSet bits) {
        for(int i = 0; i < bits.length(); ++i) {
            buffer.put(bits.get(i) ? (byte) '1' : (byte) '0');
        }
    }
}
