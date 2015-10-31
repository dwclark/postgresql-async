package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgType;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import static db.postgresql.async.serializers.SerializationContext.*;

public class LocalDateSerializer extends Serializer<LocalDate> {

    private static final String STR = "uuuu-MM-dd";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final PgType PGTYPE =
        new PgType.Builder().name("date").oid(1082).arrayId(1182).build();

    public Class<LocalDate> getType() { return LocalDate.class; }

    public static final LocalDateSerializer instance = new LocalDateSerializer();
    
    public LocalDate fromString(final String str) {
        return LocalDate.parse(str, DATE);
    }
    
    public String toString(final LocalDate val) {
        return (val == null) ? null : val.format(DATE);
    }
}
