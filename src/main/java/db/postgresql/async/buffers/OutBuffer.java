package db.postgresql.async.buffers;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.CharsetEncoder;

public interface OutBuffer {

    OutBuffer putAsciiNullString(String val);
    OutBuffer put(byte val);
    OutBuffer putShort(short val);
    OutBuffer putInt(int val);
    OutBuffer putLong(long val);
    OutBuffer putFloat(float val);
    OutBuffer putDouble(double val);
    OutBuffer put(byte[] val);
    OutBuffer put(ByteBuffer val);
    OutBuffer put(CharBuffer val);
    OutBuffer put(String val);
}
