package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgId;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import static db.postgresql.async.serializers.SerializationContext.*;

@PgId("time")
public class LocalTimeSerializer extends Serializer<LocalTime> {

    private static final String STR = "HH:mm:ss.n";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final LocalTimeSerializer instance = new LocalTimeSerializer();

    public Class<LocalTime> getType() { return LocalTime.class; }
    
    public LocalTime fromString(final String str) {
        return LocalTime.parse(str + "000", DATE);
    }

    public String toString(final LocalTime val) {
        return val == null ? null : val.format(DATE);
    }
}
