package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgType;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import static db.postgresql.async.serializers.SerializationContext.*;

public class LocalDateTimeSerializer extends Serializer<LocalDateTime> {

    private static final String STR = "uuuu-MM-dd HH:mm:ss.n";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final LocalDateTimeSerializer instance = new LocalDateTimeSerializer();

    public static final PgType PGTYPE =
        new PgType.Builder().name("timestamp").oid(1114).arrayId(1115).build();

    public Class<LocalDateTime> getType() { return LocalDateTime.class; }
    
    public LocalDateTime fromString(final String str) {
        return LocalDateTime.parse(str + "000", DATE);
    }

    public String toString(final LocalDateTime val) {
        return val == null ? null : val.format(DATE);
    }
}
