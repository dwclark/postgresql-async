package db.postgresql.async.buffers;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.List;
import java.util.ArrayList;

public class CountingOutBuffer implements OutBuffer {

    private int total = 0;
    private List<Chunk> chunks;

    public CountingOutBuffer chunk() {
        if(chunks == null) {
            chunks = new ArrayList<>();
        }

        chunks.add(new Chunk(chunks.size()));
        return this;
    }

    private CountingOutBuffer increment(final int val) {
        total += val;
        if(chunks != null) {
            chunks.get(chunks.size() - 1).plus(val);
        }

        return this;
    }
    
    public CountingOutBuffer putNullString(String val) {
        increment(val.length());
        putNull();
        return this;
    }
    
    public CountingOutBuffer put(byte val) {
        return increment(1);
    }
    
    public CountingOutBuffer putShort(short val) {
        return increment(2);
    }
    
    public CountingOutBuffer putInt(int val) {
        return increment(4);
    }
    
    public CountingOutBuffer putLong(long val) {
        return increment(8);
    }
    
    public CountingOutBuffer putFloat(float val) {
        return increment(4);
    }
    
    public CountingOutBuffer putDouble(double val) {
        return increment(8);
    }
    
    public CountingOutBuffer put(byte[] val) {
        return increment(val.length);
    }
    
    public CountingOutBuffer put(ByteBuffer val) {
        return increment(val.remaining());
    }
}
