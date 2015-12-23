package db.postgresql.async.types;

import java.time.*;
import spock.lang.*;

class IntervalTest extends Specification {

    def "Basics"() {
        setup:
        def duration = Duration.ofMillis(1_000_000L);
        def period = Period.of(2, 5, 7);

        expect:
        new Interval(period, duration) == new Interval(period, duration);
        new Interval(period, duration).hashCode() == new Interval(period, duration).hashCode();
        new Interval(period, duration).toString() == new Interval(period, duration).toString();
        
        new Interval(period) == new Interval(period);
        new Interval(period).hashCode() == new Interval(period).hashCode();
        new Interval(period).toString() == new Interval(period).toString();

        new Interval(duration) == new Interval(duration);
        new Interval(duration).hashCode() == new Interval(duration).hashCode();
        new Interval(duration).toString() == new Interval(duration).toString();

        new Interval(duration) != new Interval(period);
        new Interval(duration).hashCode() != new Interval(period).hashCode();
        new Interval(duration).toString() != new Interval(period).toString();
    }
}
