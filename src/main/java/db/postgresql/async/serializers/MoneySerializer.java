package db.postgresql.async.serializers;

import db.postgresql.async.pginfo.PgId;
import db.postgresql.async.types.Money;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Locale;
import static db.postgresql.async.serializers.SerializationContext.*;

@PgId("money")
public class MoneySerializer extends Serializer<Money> {

    private final Locale locale;

    public MoneySerializer(final Locale locale) {
        this.locale = locale;
    }

    public Class<Money> getType() { return Money.class; }
    
    public Money fromString(final String str) {
        return Money.wrap((BigDecimal) getFormatter().parse(str, new ParsePosition(0)));
    }

    public String toString(final Money val) {
        StringBuffer sb = new StringBuffer();
        getFormatter().format(val.unwrap(), sb, new FieldPosition(0));
        return sb.toString();
    }
    
    public DecimalFormat getFormatter() {
        DecimalFormat formatter = (DecimalFormat) NumberFormat.getCurrencyInstance(locale);
        formatter.setParseBigDecimal(true);
        return formatter;
    }
}
