package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgId;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Locale;
import static db.postgresql.async.serializers.SerializationContext.*;

@PgId("numeric")
public class NumericSerializer extends Serializer<BigDecimal> {

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
}
