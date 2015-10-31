package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgType;
import java.nio.ByteBuffer;
import static db.postgresql.async.serializers.SerializationContext.*;

public class StringSerializer extends Serializer<String> {

    public static final PgType PGTYPE_TEXT =
        new PgType.Builder().name("text").oid(25).arrayId(1009).build();
    public static final PgType PGTYPE_VARCHAR =
        new PgType.Builder().name("varchar").oid(1043).arrayId(1015).build();

    public Class<String> getType() { return String.class; }
    public Class getArrayType() { return String[].class; }

    public String fromString(final String str) {
        return str;
    }

    public String toString(final String str) {
        return str;
    }
    
    public String read(final ByteBuffer buffer, final int size) {
        return isNull(size) ? null : bufferToString(buffer, size);
    }
    
    public void write(final ByteBuffer buffer, final String val) {
        if(val != null) {
            stringToBuffer(buffer, val);
        }
    }
}
