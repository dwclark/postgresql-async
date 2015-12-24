package db.postgresql.async.serializers;

import java.util.Collections;
import java.util.List;
import java.nio.ByteBuffer;
import db.postgresql.async.types.Interval;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class IntervalSerializer extends Serializer<Interval> {

    private IntervalSerializer() { }
    
    public static final IntervalSerializer instance = new IntervalSerializer();

    public Class<Interval> getType() { return Interval.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.interval");
    }
    
    public Interval read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.BINARY) {
            return new Interval(buffer);
        }
        else {
            throw new UnsupportedOperationException("Deserialization from text type not supported for intervals");
        }
    }

    public void write(final ByteBuffer buffer, final Interval i, final Format format) {
        if(i == null) {
            putNull(buffer);
            return;
        }
        
        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> i.toBuffer(b));
        }
        else {
            throw new UnsupportedOperationException("Serialization to text type not supported for intervals");
        }
    }
}
