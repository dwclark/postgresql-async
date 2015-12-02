package db.postgresql.async.serializers;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.LocalDateTime;
import java.time.temporal.JulianFields;

public class PostgresDateTime {

    private static final long NANOS_PER_MICROSECOND = 1000L;
    private static final long MICROSECONDS_PER_DAY = 86_400_000_000L;
    private static final LocalDate EPOCH = LocalDate.of(2000, 1, 1);
    private static final long EPOCH_LONG = LocalDate.of(2000, 1, 1).getLong(JulianFields.JULIAN_DAY);
    
    public static long toDay(final LocalDate date) {
        return date.getLong(JulianFields.JULIAN_DAY) - EPOCH_LONG;
    }

    public static LocalDate toLocalDate(final long day) {
        return EPOCH.with(JulianFields.JULIAN_DAY, day + EPOCH_LONG);
    }

    public static long toTime(final LocalTime time) {
        return time.toNanoOfDay() / 1000L;
    }

    public static LocalTime toLocalTime(final long time) {
        return LocalTime.ofNanoOfDay(time * 1000L);
    }

    public static OffsetTime toOffsetTime(final long time, final int offset) {
        System.out.println("Time: " + time + " offset: " + offset);
        return OffsetTime.of(toLocalTime(time), ZoneOffset.ofTotalSeconds(offset));
    }

    public static int toSeconds(final ZoneOffset offset) {
        return offset.getTotalSeconds();
    }

    public static ZoneOffset toOffset(final int seconds) {
        System.out.println("Offset seconds: " + seconds);
        return ZoneOffset.ofTotalSeconds(seconds);
    }

    public static long toTimestamp(final LocalDateTime ldt) {
        return (toDay(ldt.toLocalDate()) * MICROSECONDS_PER_DAY) + toTime(ldt.toLocalTime());
    }

    public static LocalDateTime toLocalDateTime(final long total) {
        final long days = total / MICROSECONDS_PER_DAY;
        final LocalDate localDate = toLocalDate(days < 0 ? days - 1 : days);
        final long leftover = total - (days * MICROSECONDS_PER_DAY);
        final long time = (leftover < 0) ? MICROSECONDS_PER_DAY + leftover : leftover;
        final LocalTime localTime = toLocalTime(time);
        return LocalDateTime.of(localDate, localTime);
    }
}
