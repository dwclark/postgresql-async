package db.postgresql.async.types;

import java.time.Period;
import java.time.Duration;
import java.nio.ByteBuffer;

public class Interval {

    private final Period period;
    public Period getPeriod() { return period; }
    
    private final Duration duration;
    public Duration getDuration() { return duration; }

    public Interval(final Period period, final Duration duration) {
        this.period = period;
        this.duration = duration;
    }

    public Interval(final Period period) {
        this(period, Duration.ZERO);
    }

    public Interval(final Duration duration) {
        this(Period.ZERO, duration);
    }

    public Interval(final ByteBuffer buffer) {
        this.duration = toDuration(buffer.getLong());
        this.period = toPeriod(buffer.getInt(), buffer.getInt());
    }

    public void toBuffer(final ByteBuffer buffer) {
        buffer.putLong(toMicroSeconds(duration));
        buffer.putInt(period.getDays());
        buffer.putInt(toMonths(period));
    }

    public static Interval read(final int size, final ByteBuffer buffer, final int oid) {
        return new Interval(buffer);
    }

    public static void write(final ByteBuffer buffer, final Object o) {
        ((Interval) o).toBuffer(buffer);
    }

    @Override
    public boolean equals(final Object rhs) {
        return (rhs instanceof Interval) ? equals((Interval) rhs) : false;
    }

    public boolean equals(final Interval rhs) {
        return period.equals(rhs.period) && duration.equals(rhs.duration);
    }

    @Override
    public int hashCode() {
        return 431 * (937 + duration.hashCode()) + period.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s %s", period.toString(), duration.toString());
    }

    public static int toMonths(final Period period) {
        return (period.getYears() * 12) + period.getMonths();
    }

    public static Period toPeriod(final int days, final int months) {
        return Period.of(months / 12, months % 12, days);
    }

    public static long toMicroSeconds(final Duration duration) {
        return (duration.getSeconds() * 1_000_000L) + (duration.getNano() / 1_000);
    }

    public static Duration toDuration(final long microSeconds) {
        final long seconds = microSeconds / 1_000_000;
        final long nanos = (microSeconds - (seconds * 1_000_000)) * 1_000;
        return Duration.ofSeconds(seconds, nanos);
    }
}
