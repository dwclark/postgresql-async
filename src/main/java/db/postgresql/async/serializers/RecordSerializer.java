package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgType;
import db.postgresql.async.messages.Format;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import db.postgresql.async.types.Record;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class RecordSerializer extends Serializer<Record> {

    final PgType pgType;
    
    public RecordSerializer(final PgType pgType) {
        this.pgType = pgType;
    }

    public Class<Record> getType() { return Record.class; }

    public List<String> getPgNames() {
        return Collections.emptyList(); //handles all record types
    }

    public Record read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.BINARY) {
            return new Record(pgType, buffer);
        }
        else {
            throw new UnsupportedOperationException("Text deserialization of records not supported");
        }
    }

    public void write(final ByteBuffer buffer, final Record val, final Format format) {
        if(val == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> val.toBuffer(b));
        }
        else {
            throw new UnsupportedOperationException("Text serialization of records not supported");
        }
    }
}
