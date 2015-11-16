package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Collections;

public class BytesSerializer extends Serializer<byte[]> {

    public static final BytesSerializer instance = new BytesSerializer();

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.bytea");
    }
    
    public Class<byte[]> getType() { return byte[].class; }
    
    public byte[] fromString(final String str) {
        byte[] ret = new byte[(str.length() - 2) / 2];
        for(int i = 2; i < ret.length; i = i + 2) {
            ret[i] = (byte) ((Character.digit(str.charAt(i), 16) << 4) + Character.digit(str.charAt(i+1), 16));
        }
        
        return ret;
    }

    public String toString(final byte[] bytes) {
        StringBuilder sb = new StringBuilder(2+ (bytes.length * 2));
        sb.append("\\x");
        for(byte b : bytes) {
            sb.append(asciiCharArray[b]);
        }

        return sb.toString();
    }
    
    public byte[] read(final ByteBuffer buffer, final int size) {
        if(isNull(size)) {
            return null;
        }

        buffer.get(); buffer.get(); //advance past escape sequence
        byte[] ret = new byte[(size - 2) / 2];
        for(int i = 0; i < ret.length; ++i) {
            ret[i] = (byte) ((Character.digit(buffer.get(), 16) << 4) + Character.digit(buffer.get(), 16));
        }

        return ret;
    }

    final private static char[] asciiCharArray = { '0', '1', '2', '3', '4',
                                                   '5', '6', '7', '8', '9',
                                                   'a', 'b', 'c', 'd', 'e', 'f' };
    
    final private static byte[] asciiHexArray;

    static {
        asciiHexArray = new byte[asciiCharArray.length];
        for(int i = 0; i < asciiCharArray.length; ++ i) {
            asciiHexArray[i] = (byte) asciiCharArray[i];
        }
    }
    
    public void write(final ByteBuffer buffer, final byte[] bytes) {
        buffer.put((byte) '\\').put((byte) 'x');
        for(int i = 0; i < bytes.length; ++i) {
            buffer.put(asciiHexArray[bytes[i] >>> 4]);
            buffer.put(asciiHexArray[bytes[i] & 0x0F]);
        }
    }
}
