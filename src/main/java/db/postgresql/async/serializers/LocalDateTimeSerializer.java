package db.postgresql.async.serializers;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

public class LocalDateTimeSerializer extends Serializer<LocalDateTime> {

    private static final String STR = "uuuu-MM-dd HH:mm:ss.n";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public static final LocalDateTimeSerializer instance = new LocalDateTimeSerializer();

    public Class<LocalDateTime> getType() { return LocalDateTime.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.timestamp");
    }

    public LocalDateTime fromString(final String str) {
        return LocalDateTime.parse(str + "000", DATE);
    }

    public String toString(final LocalDateTime val) {
        return val == null ? null : val.format(DATE);
    }
}
