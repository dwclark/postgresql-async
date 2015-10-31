package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgType;
import java.nio.ByteBuffer;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import static db.postgresql.async.serializers.SerializationContext.*;

public class OffsetTimeSerializer extends Serializer<OffsetTime> {
    
    private static final String STR = "HH:mm:ss.nx";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final PgType PGTYPE =
         new PgType.Builder().name("timetz").oid(1266).arrayId(1270).build();

    public static final OffsetTimeSerializer instance = new OffsetTimeSerializer();
    
    public Class<OffsetTime> getType() { return OffsetTime.class; }

    public OffsetTime fromString(final String str) {
        final int index = str.lastIndexOf('-');
        return OffsetTime.parse(str.substring(0, index) + "000" + str.substring(index), DATE);
    }

    public String toString(final OffsetTime val) {
        return val == null ? null : val.format(DATE);
    }
}
