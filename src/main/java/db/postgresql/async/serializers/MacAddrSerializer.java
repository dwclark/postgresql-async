package db.postgresql.async.serializers;

import db.postgresql.async.messages.Format;
import db.postgresql.async.types.MacAddr;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class MacAddrSerializer extends Serializer<MacAddr> {

    public static final MacAddrSerializer instance = new MacAddrSerializer();
    
    public Class<MacAddr> getType() { return MacAddr.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.macaddr");
    }

    public MacAddr fromString(final String str) {
        return MacAddr.fromString(str);
    }

    public String toString(final MacAddr val) {
        return val == null ? null : val.toString();
    }

    public MacAddr read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.TEXT) {
            buffer.position(buffer.position() - 4);
            return MacAddr.fromString(bufferToString(buffer));
        }
        else {
            return new MacAddr(buffer);
        }
    }

    public void write(final ByteBuffer buffer, final MacAddr addr, final Format format) {
        if(addr == null) {
            putNull(buffer);
            return;
        }

        if(format == Format.TEXT) {
            putWithSize(buffer, (b) -> stringToBuffer(b, addr.toString()));
        }
        else {
            putWithSize(buffer, (b) -> addr.toBuffer(b));
        }
    }
}
