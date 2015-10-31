package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgType;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Locale;
import static db.postgresql.async.serializers.SerializationContext.*;

public class NumericSerializer extends Serializer<BigDecimal> {

    public static final PgType PGTYPE =
        new PgType.Builder().name("numeric").oid(1700).arrayId(1231).build();

    private final Locale locale;

    public NumericSerializer(final Locale locale) {
        this.locale = locale;
    }

    public Class<BigDecimal> getType() { return BigDecimal.class; }

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
    
    public BigDecimal read(final ByteBuffer buffer, final int size) {
        return isNull(size) ? null : fromString(bufferToString(buffer, size));
    }

    public void write(final ByteBuffer buffer, final BigDecimal bd) {
        stringToBuffer(buffer, toString(bd));
    }
}