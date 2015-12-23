package db.postgresql.async.serializers;

import java.time.*;
import spock.lang.*;
import static db.postgresql.async.serializers.PostgresDateTime.*;

class PostgresNumericTest extends Specification {

    def "Test Reversible"() {
        expect:
        1234.567 == new PostgresNumeric(1234.567).toBigDecimal();
        1234.5678 == new PostgresNumeric(1234.5678).toBigDecimal();
        175123.123456 == new PostgresNumeric(175123.123456).toBigDecimal();
        3.14 == new PostgresNumeric(3.14).toBigDecimal(); 
    }
}
