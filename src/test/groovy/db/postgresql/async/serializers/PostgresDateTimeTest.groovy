package db.postgresql.async.serializers;

import java.time.*;
import spock.lang.*;
import static db.postgresql.async.serializers.PostgresDateTime.*;

class PostgresDateTimeTest extends Specification {

    def "Test Reversible Local Date"() {
        setup:
        LocalDate today = LocalDate.now();
        LocalDate starTrek = LocalDate.of(2350, 10, 10);
        LocalDate jesus = LocalDate.of(1, 12, 25);
        
        expect:
        today == toLocalDate(toDay(today));
        starTrek == toLocalDate(toDay(starTrek));
        jesus == toLocalDate(toDay(jesus));
    }

    def "Test Reversible Local Time"() {
        setup:
        LocalTime now = LocalTime.now().withNano(500_000);
        LocalTime big = LocalTime.of(23, 59, 59, 5000);
        LocalTime maxMicros = LocalTime.of(23, 59, 59, 999999000);
        
        expect:
        now == toLocalTime(toTime(now));
        LocalTime.MIN == toLocalTime(toTime(LocalTime.MIN));
        maxMicros == toLocalTime(toTime(maxMicros));
        big == toLocalTime(toTime(big));
        LocalTime.NOON == toLocalTime(toTime(LocalTime.NOON));
    }

    def "Test Reversible Local Date Time"() {
        setup:
        LocalDateTime now = LocalDateTime.now().withNano(500_000);
        LocalDateTime starTrek = LocalDateTime.of(2350, 10, 10, 13, 17, 22, 3_450_000);
        LocalDateTime jesus = LocalDateTime.of(25, 4, 4, 7, 30, 21, 913_000_000);
        
        expect:
        now == toLocalDateTime(toTimestamp(now));
        starTrek == toLocalDateTime(toTimestamp(starTrek));
        jesus == toLocalDateTime(toTimestamp(jesus));
    }

    def "Test Reversible Epoch Conversion"() {
        setup:
        final long now = System.currentTimeMillis();
        final long starTrek = OffsetDateTime.of(2350, 10, 10, 13, 17, 22, 3_450_000, ZoneOffset.UTC).toInstant().toEpochMilli();
        final long jesus = OffsetDateTime.of(25, 4, 4, 7, 30, 21, 913_000_000, ZoneOffset.UTC).toInstant().toEpochMilli();
        
        expect:
        now == p2j(j2p(now));
        starTrek == p2j(j2p(starTrek));
        jesus == p2j(j2p(jesus));
    }
}
