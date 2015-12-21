package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import db.postgresql.async.types.Money;
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

public class MoneySerializer extends Serializer<Money> {

    private final Locale locale;

    public MoneySerializer(final Locale locale) {
        this.locale = locale;
    }

    public Class<Money> getType() { return Money.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.money");
    }
    
    public Money fromString(final String str) {
        return new Money((BigDecimal) getFormatter().parse(str, new ParsePosition(0)));
    }

    public String toString(final Money val) {
        StringBuffer sb = new StringBuffer();
        getFormatter().format(val.toBigDecimal(), sb, new FieldPosition(0));
        return sb.toString();
    }
    
    public DecimalFormat getFormatter() {
        DecimalFormat formatter = (DecimalFormat) NumberFormat.getCurrencyInstance(locale);
        formatter.setParseBigDecimal(true);
        return formatter;
    }
    
    public Money read(final ByteBuffer buffer, final Format format) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(format == Format.BINARY) {
            return new Money(buffer.getLong());
        }
        else {
            buffer.position(buffer.position() - 4);
            return fromString(bufferToString(buffer));
        }
    }

    public void write(final ByteBuffer buffer, final Money m, final Format format) {
        if(m == null) {
            putNull(buffer);
            return;
        }
        
        if(format == Format.BINARY) {
            putWithSize(buffer, (b) -> b.putLong(m.longValue()));
        }
        else {
            putWithSize(buffer, (b) -> stringToBuffer(b, toString(m)));
        }
    }
}
