package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgType;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import static db.postgresql.async.serializers.SerializationContext.*;

public class OffsetDateTimeSerializer extends Serializer<OffsetDateTime> {

    private static final String STR = "uuuu-MM-dd HH:mm:ss.nx";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final PgType PGTYPE =
        new PgType.Builder().name("timestamptz").oid(1184).arrayId(1185).build();

    public static final OffsetDateTimeSerializer instance = new OffsetDateTimeSerializer();
    
    public Class<OffsetDateTime> getType() { return OffsetDateTime.class; }

    public OffsetDateTime fromString(final String str) {
        final int index = str.lastIndexOf('-');
        return OffsetDateTime.parse(str.substring(0, index) + "000" + str.substring(index), DATE);
    }

    public String toString(final OffsetDateTime val) {
        return (val == null) ? null : val.format(DATE);
    }
}
