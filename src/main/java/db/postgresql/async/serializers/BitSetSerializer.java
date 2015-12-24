package db.postgresql.async.serializers;

import db.postgresql.async.messages.Format;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
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

    private static final int MASK = 0b1000_0000;
    
    private static BitSet from(final byte[] bytes, final int num) {
        int mask = MASK;
        int bytesIndex = 0;
        final BitSet ret = new BitSet(num);
        
        for(int i = 0; i < num; ++i) {
            final int tmp = bytes[bytesIndex] & mask;
            if(tmp != 0) {
                ret.set(i);
            }
            
            if(i != 0 && i % 8 == 0) {
                mask = MASK;
                ++bytesIndex;
            }
            else {
                mask = mask >> 1;
            }
        }

        return ret;
    }

    private static byte[] from(final BitSet bitSet) {
        final byte[] ret = new byte[sizeInBytes(bitSet.length())];
        int mask = MASK;
        int bytesIndex = 0;
        
        for(int i = 0; i < bitSet.length(); ++i) {
            if(bitSet.get(i)) {
                ret[bytesIndex] = (byte) (ret[bytesIndex] | mask);
            }

            if(i != 0 && i % 8 == 0) {
                mask = MASK;
                ++bytesIndex;
            }
            else {
                mask = mask >> 1;
            }
        }

        return ret;
    }

    private static int sizeInBytes(final int num) {
        if(num <= 8) {
            return 1;
        }

        final int tmp = num / 8;
        return tmp + ((num % 8 == 0) ? 0 : 1);
    }
    
    public BitSet read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }

        if(format == Format.TEXT) {
            buffer.position(buffer.position() - 4);
            return fromString(bufferToString(buffer));
        }
        else {
            final int num = buffer.getInt();
            byte[] bytes = new byte[sizeInBytes(num)];
            buffer.get(bytes);
            return from(bytes, num);
        }
    }

    public void write(final ByteBuffer buffer, final BitSet bits, final Format format) {
        if(bits == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.TEXT) {
            putWithSize(buffer, (b) -> stringToBuffer(b, toString(bits)));
        }
        else {
            putWithSize(buffer, (b) -> {
                    b.putInt(bits.length());
                    b.put(from(bits));
                });
        }
    }
}
