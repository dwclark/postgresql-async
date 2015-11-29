package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import db.postgresql.async.types.MacAddr;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

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
        throw new UnsupportedOperationException();
    }

    public void write(final ByteBuffer buffer, final MacAddr addr, final Format format) {
        throw new UnsupportedOperationException();
    }
}
