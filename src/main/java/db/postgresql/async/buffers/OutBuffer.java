package db.postgresql.async.buffers;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.CharsetEncoder;

public interface OutBuffer {

    default OutBuffer putNull() {
        put((byte) 0);
        return this;
    }

    default OutBuffer put(final CharBuffer val, final Charset cset) {
        final byte[] tmp = new byte[128];
        final ByteBuffer buf = ByteBuffer.wrap(tmp);
        final CharsetEncoder encoder = cset.newEncoder();
        final CharBuffer ro = val.asReadOnlyBuffer();
        CoderResult result = CoderResult.OVERFLOW;
        while(result == CoderResult.OVERFLOW) {
            result = encoder.encode(val, buf, false);
            buf.flip();
            put(buf);
            buf.clear();
        }

        result = CoderResult.OVERFLOW;
        while(result == CoderResult.OVERFLOW) {
            encoder.encode(val, buf, true);
            buf.flip();
            put(buf);
            buf.clear();
        }

        result = CoderResult.OVERFLOW;
        while(result == CoderResult.OVERFLOW) {
            result = encoder.flush(buf);
            buf.flip();
            put(buf);
            buf.clear();
        }

        return this;
    }
    
    OutBuffer putNullString(String val);
    OutBuffer put(byte val);
    OutBuffer putShort(short val);
    OutBuffer putInt(int val);
    OutBuffer putLong(long val);
    OutBuffer putFloat(float val);
    OutBuffer putDouble(double val);
    OutBuffer put(byte[] val);
    OutBuffer put(ByteBuffer val);
}
