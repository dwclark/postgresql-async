package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgId;
import java.nio.ByteBuffer;
import static db.postgresql.async.serializers.SerializationContext.*;

@PgId({"text","varchar"})
public class StringSerializer extends Serializer<String> {

    private StringSerializer() { }

    public static final StringSerializer instance = new StringSerializer();

    public Class<String> getType() { return String.class; }
    public Class getArrayType() { return String.class; }

    public String fromString(final String str) {
        return str;
    }

    public String toString(final String str) {
        return str;
    }
}
