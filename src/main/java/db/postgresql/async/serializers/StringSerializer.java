package db.postgresql.async.serializers;

import java.util.Arrays;
import java.util.List;

public class StringSerializer extends Serializer<String> {

    private StringSerializer() { }

    public static final StringSerializer instance = new StringSerializer();

    public Class<String> getType() { return String.class; }
    public Class getArrayType() { return String.class; }

    public List<String> getPgNames() {
        return Arrays.asList("pg_catalog.text", "pg_catalog.varchar", "pg_catalog.xml",
                             "pg_catalog.json", "pg_catalog.jsonb");
    }

    public String fromString(final String str) {
        return str;
    }

    public String toString(final String str) {
        return str;
    }
}
