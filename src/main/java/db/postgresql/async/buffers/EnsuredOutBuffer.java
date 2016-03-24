package db.postgresql.async.buffers;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import static db.postgresql.async.buffers.BufferOps.*;
import static java.nio.charset.StandardCharsets.US_ASCII;

public class EnsuredOutBuffer implements OutBuffer {

    private ByteBuffer buffer;
    
    public EnsuredOutBuffer(final ByteBuffer buffer) {
        this.buffer = buffer;
    }
    
    public EnsuredOutBuffer putNullString(final String val) {
        buffer = ensure(buffer, val.length() + 1).put(val.getBytes(US_ASCII)).put((byte) 0);
        return this;
    }
    
    public EnsuredOutBuffer put(final byte val) {
        buffer = ensure(buffer, 1).put((byte) 0);
        return this;
    }
    
    public EnsuredOutBuffer putShort(final short val) {
        buffer = ensure(buffer, 2).putShort(val);
        return this;
    }
    
    public EnsuredOutBuffer putInt(final int val) {
        buffer = ensure(buffer, 4).putInt(val);
        return this;
    }
    
    public EnsuredOutBuffer putLong(final long val) {
        buffer = ensure(buffer, 8).putLong(val);
        return this;
    }
    
    public EnsuredOutBuffer putFloat(final float val) {
        buffer = ensure(buffer, 4).putFloat(val);
        return this;
    }
        
    public EnsuredOutBuffer putDouble(final double val) {
        buffer = ensure(buffer, 8).putDouble(val);
        return this;
    }
    
    public EnsuredOutBuffer put(final byte[] val) {
        buffer = ensure(buffer, val.length).put(val);
        return this;
    }
    
    public EnsuredOutBuffer put(final ByteBuffer val) {
        buffer = ensure(buffer, val.remaining()).put(val);
        return this;
    }
}
