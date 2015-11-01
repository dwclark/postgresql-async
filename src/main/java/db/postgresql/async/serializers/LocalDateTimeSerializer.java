package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgId;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import static db.postgresql.async.serializers.SerializationContext.*;

@PgId("timestamp")
public class LocalDateTimeSerializer extends Serializer<LocalDateTime> {

    private static final String STR = "uuuu-MM-dd HH:mm:ss.n";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final LocalDateTimeSerializer instance = new LocalDateTimeSerializer();

    public Class<LocalDateTime> getType() { return LocalDateTime.class; }
    
    public LocalDateTime fromString(final String str) {
        return LocalDateTime.parse(str + "000", DATE);
    }

    public String toString(final LocalDateTime val) {
        return val == null ? null : val.format(DATE);
    }
}
