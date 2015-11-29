package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import db.postgresql.async.types.Inet;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

public class InetSerializer extends Serializer<Inet> {

    public static final InetSerializer instance = new InetSerializer();
    
    public Class<Inet> getType() { return Inet.class; }

    public List<String> getPgNames() {
        return Arrays.asList("pg_catalog.inet", "pg_catalog.cidr");
    }

    public Inet fromString(final String str) {
        return Inet.fromString(str);
    }

    public String toString(final Inet val) {
        return val == null ? null : val.toString();
    }
    
    public Inet read(final ByteBuffer buffer, final Format format) {
        throw new UnsupportedOperationException();
    }

    public void write(final ByteBuffer buffer, final Inet inet, final Format format) {
        throw new UnsupportedOperationException();
    }
}
