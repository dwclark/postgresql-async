package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.util.BitSet;

public class BitSetSerializer {
    
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
    
    public static BitSet read(final int size, final ByteBuffer buffer, final int oid) {
        final int num = buffer.getInt();
        final byte[] bytes = new byte[sizeInBytes(num)];
        buffer.get(bytes);
        return from(bytes, num);
    }

    public static void write(final ByteBuffer buffer, final Object o) {
        final BitSet bits = (BitSet) o;
        buffer.putInt(bits.length());
        buffer.put(from(bits));
    }
}
