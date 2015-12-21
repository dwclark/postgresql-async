package db.postgresql.async.types;

import java.math.BigDecimal;

public class Money {

    private static final int DEFAULT_SCALE = 2;
    
    private final long value;

    public Money(final long value) {
        this.value = value;
    }

    public Money(final BigDecimal bd) {
        this.value = bd.movePointRight(bd.scale()).longValueExact();
    }

    public long longValue() {
        return value;
    }

    public BigDecimal toBigDecimal() {
        return toBigDecimal(DEFAULT_SCALE);
    }

    public BigDecimal toBigDecimal(final int scale) {
        return BigDecimal.valueOf(scale);
    }

    public Money plus(final Money rhs) {
        return new Money(value + rhs.value);
    }

    public Money minus(final Money rhs) {
        return new Money(value - rhs.value);
    }

    public Money multiply(final Money rhs) {
        return new Money(value * rhs.value);
    }

    public double div(final Money rhs) {
        return ((double) value) / rhs.value;
    }

    public Money negative() {
        return new Money(-value);
    }

    public Money positive() {
        return value > 0 ? this : new Money(-this.value);
    }

    public Object asType(final Class type) {
        if(type == BigDecimal.class) {
            return toBigDecimal();
        }
        else if(type == Long.class || type == long.class) {
            return value;
        }
        else {
            throw new ClassCastException("Cannot convert Money to " + type.getName());
        }
    }
    
    @Override
    public boolean equals(Object rhs) {
        if(!(rhs instanceof Money)) {
            return false;
        }

        Money money = (Money) rhs;
        return value == money.value;
    }

    @Override
    public int hashCode() {
        return ((int) value) ^ (int) (value >>> 32);
    }
}
