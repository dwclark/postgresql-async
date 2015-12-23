package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.messages.Format.*;
import static db.postgresql.async.buffers.BufferOps.*;
import static db.postgresql.async.serializers.SerializationContext.*;

public class NumericSerializer extends Serializer<BigDecimal> {

    private final Locale locale;

    public NumericSerializer(final Locale locale) {
        this.locale = locale;
    }

    public Class<BigDecimal> getType() { return BigDecimal.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.numeric");
    }

    public DecimalFormat getFormatter() {
        DecimalFormat formatter = (DecimalFormat) NumberFormat.getInstance(locale);
        formatter.setParseBigDecimal(true);
        return formatter;
    }

    public BigDecimal fromString(final String str) {
        return (BigDecimal) getFormatter().parse(str, new ParsePosition(0));
    }

    public String toString(final BigDecimal bd) {
        StringBuffer sb = new StringBuffer();
        getFormatter().format(bd, sb, new FieldPosition(0));
        return sb.toString();
    }

    public BigDecimal read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }

        if(format == Format.TEXT) {
            buffer.position(buffer.position() - 4);
            return fromString(bufferToString(buffer));
        }
        else {
            return new PostgresNumeric(buffer).toBigDecimal();
        }
    }

    public void write(final ByteBuffer buffer, final BigDecimal bd, final Format format) {
        if(bd == null) {
            putNull(buffer);
        }

        if(format == Format.TEXT) {
            putWithSize(buffer, (b) -> stringToBuffer(b, toString(bd)));
        }
        else {
            putWithSize(buffer, (b) -> new PostgresNumeric(bd).toBuffer(b));
        }
    }
}
