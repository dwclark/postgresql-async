package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;

public class LocalDateSerializer extends Serializer<LocalDate> {

    private static final String STR = "uuuu-MM-dd";
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern(STR);

    public Class<LocalDate> getType() { return LocalDate.class; }

    @Override
    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.date");
    }

    public static final LocalDateSerializer instance = new LocalDateSerializer();
    
    public LocalDate fromString(final String str) {
        return LocalDate.parse(str, DATE);
    }
    
    public String toString(final LocalDate val) {
        return (val == null) ? null : val.format(DATE);
    }

    public LocalDate read(final ByteBuffer buffer, final Format format) {
        throw new UnsupportedOperationException();
    }

    public void write(final ByteBuffer buffer, final LocalDate date, final Format format) {
        throw new UnsupportedOperationException();
    }
}
