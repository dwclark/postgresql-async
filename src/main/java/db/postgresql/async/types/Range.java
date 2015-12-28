package db.postgresql.async.types;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.LocalDate;
import java.util.function.Function;
import static db.postgresql.async.serializers.Primitives.*;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.types.Hashing.*;

public abstract class Range {

    private final static Function<Object,String> EMPTY_TO_STRING = (o) -> "";
    private final static Function<Object,String> TO_STRING = (o) -> o.toString();
    
    public enum Bound {
        EMPTY(' ',' ', EMPTY_TO_STRING, EMPTY_TO_STRING),
        INCLUSIVE('[',']', TO_STRING, TO_STRING),
        EXCLUSIVE('(',')', TO_STRING, TO_STRING),
        INFINITE('(',')', (o) -> "-infinity", (o) -> "infinity");
        
        private Bound(final char lowerChar, final char upperChar,
                      final Function<Object,String> lower, final Function<Object,String> upper) {
            this.lowerChar = lowerChar;
            this.upperChar = upperChar;
            this.lower = lower;
            this.upper = upper;
        }
        
        public boolean exists() {
            return this == INCLUSIVE || this == EXCLUSIVE;
        }

        final char lowerChar;
        final char upperChar;
        final Function<Object,String> lower;
        final Function<Object,String> upper;
    }
    
    private static final int EMPTY = 0x01;
    private static final int LB_INC = 0x02;
    private static final int UB_INC = 0x04;
    private static final int LB_INF = 0x08;
    private static final int UB_INF = 0x10;
    private static final int LB_NULL = 0x20;
    private static final int UB_NULL = 0x40;

    protected final int flags;
    
    public boolean isEmpty() {
        return (EMPTY & flags) > 0;
    }

    public Bound getLowerBound() {
        return lowerBound(flags);
    }

    public static Bound lowerBound(final int flags) {
        if((EMPTY & flags) > 0 || (LB_NULL & flags) > 0) {
            return Bound.EMPTY;
        }
        else if((LB_INF & flags) > 0) {
            return Bound.INFINITE;
        }
        else if((LB_INC & flags) > 0) {
            return Bound.INCLUSIVE;
        }
        else {
            return Bound.EXCLUSIVE;
        }
    }

    public Bound getUpperBound() {
        return upperBound(flags);
    }

    public static Bound upperBound(final int flags) {
        if((EMPTY & flags) > 0 || (UB_NULL & flags) > 0) {
            return Bound.EMPTY;
        }
        else if((UB_INF & flags) > 0) {
            return Bound.INFINITE;
        }
        else if((UB_INC & flags) > 0) {
            return Bound.INCLUSIVE;
        }
        else {
            return Bound.EXCLUSIVE;
        }
    }

    protected Range(final Bound lowerBound, final Bound upperBound) {
        this(normalize(upperFlags(lowerFlags(0, lowerBound), upperBound)));
    }

    protected Range(final int flags) {
        this.flags = flags;
    }

    private static int lowerFlags(final int mask, final Bound bound) {
        switch(bound) {
        case EMPTY: return mask | LB_NULL;
        case INFINITE: return mask | LB_INF;
        case INCLUSIVE: return mask | LB_INC;
        case EXCLUSIVE:  return mask;
        default:
            throw new IllegalArgumentException();
        }
    }

    private static int upperFlags(final int mask, final Bound bound) {
        switch(bound) {
        case EMPTY: return mask | UB_NULL;
        case INFINITE: return mask | UB_INF;
        case INCLUSIVE: return mask | UB_INC;
        case EXCLUSIVE:  return mask;
        default:
            throw new IllegalArgumentException();
        }
    }

    public abstract void toBuffer(ByteBuffer buffer);

    public static void write(final ByteBuffer buffer, final Object o) {
        ((Range) o).toBuffer(buffer);
    }

    public String toString(final Object lower, final Object upper) {
        final Bound lbound = getLowerBound();
        final Bound ubound = getUpperBound();
        if(lbound == Bound.EMPTY && lbound == Bound.EMPTY) {
            return "empty";
        }

        return String.format("%s%s,%s%s", lbound.lowerChar, lbound.lower.apply(lower), ubound.upper.apply(upper), ubound.upperChar);
    }

    private static int normalize(final int mask) {
        if(((LB_NULL & mask) > 0) && ((UB_NULL & mask) > 0)) {
            return EMPTY;
        }
        else {
            return mask;
        }
    }

    public static class Int4 extends Range {
        private final int lower;
        private final int upper;

        public int getLower() {
            return lower;
        }
        
        public int getUpper() {
            return upper;
        }
        
        private Int4(final int flags, final int lower, final int upper) {
            super(flags);
            this.lower = lower;
            this.upper = upper;
        }

        public Int4(final Bound lowerBound, final int lower,
                    final int upper, final Bound upperBound) {
            super(lowerBound, upperBound);
            this.lower = lower;
            this.upper = upper;
        }

        public static Int4 read(final int size, final ByteBuffer buffer, final int oid) {
            final int flags = 0xFF & buffer.get();
            final int lower = lowerBound(flags).exists() ? readInt(buffer) : 0;
            final int upper = upperBound(flags).exists() ? readInt(buffer) : 0;
            return new Int4(flags, lower, upper);
        }

        public void toBuffer(final ByteBuffer buffer) {
            buffer.put((byte) flags);
            if(getLowerBound().exists()) {
                writeInt(buffer, lower);
            }

            if(getUpperBound().exists()) {
                writeInt(buffer, upper);
            }
        }

        @Override
        public String toString() {
            return toString(lower, upper);
        }

        @Override
        public int hashCode() {
            return hash(hash(hash(START, flags), lower), upper);
        }

        @Override
        public boolean equals(final Object rhs) {
            return (rhs instanceof Int4) ? equals((Int4) rhs) : false;
        }

        public boolean equals(final Int4 rhs) {
            return (flags == rhs.flags &&
                    lower == rhs.lower &&
                    upper == rhs.upper);
        }
    }

    /*public static class Int8 extends Range {
        private Int8(final Object lowerValue, final Bound lowerBound,
                     final Object upperValue, final Bound upperBound) {
            super((Long) lowerValue, lowerBound, (Long) upperValue, upperBound);
        }
    }

    public static class Numeric extends Range<BigDecimal> {
        private Numeric(final Object lowerValue, final Bound lowerBound,
                        final Object upperValue, final Bound upperBound) {
            super((BigDecimal) lowerValue, lowerBound, (BigDecimal) upperValue, upperBound);
        }
    }

    public static class Timestamp extends Range<LocalDateTime> {
        private Timestamp(final Object lowerValue, final Bound lowerBound,
                          final Object upperValue, final Bound upperBound) {
            super((LocalDateTime) lowerValue, lowerBound, (LocalDateTime) upperValue, upperBound);
        }
    }

    public static class Timestampz extends Range<OffsetDateTime> {
        private Timestampz(final Object lowerValue, final Bound lowerBound,
                           final Object upperValue, final Bound upperBound) {
            super((OffsetDateTime) lowerValue, lowerBound, (OffsetDateTime) upperValue, upperBound);
        }
    }

    public static class Date extends Range<LocalDate> {
        private Date(final Object lowerValue, final Bound lowerBound,
                     final Object upperValue, final Bound upperBound) {
            super((LocalDate) lowerValue, lowerBound, (LocalDate) upperValue, upperBound);
        }
        }*/
}
