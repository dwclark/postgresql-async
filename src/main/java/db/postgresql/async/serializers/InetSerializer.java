package db.postgresql.async.serializers;

import java.util.Arrays;
import java.util.List;
import db.postgresql.async.types.Inet;

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
}
