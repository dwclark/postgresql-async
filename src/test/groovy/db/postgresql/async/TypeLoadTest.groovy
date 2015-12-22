package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.types.*;
import db.postgresql.async.*;
import java.time.*;

class TypeLoadTest extends Specification {

    @Shared Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    def "Test Load Types"() {
        expect:
        session.sessionInfo.registry.pgType(23);
        session.sessionInfo.registry.pgType(1043);
        session.sessionInfo.registry.serializer(23) == IntegerSerializer.instance;
        session.sessionInfo.registry.serializer(Integer) == IntegerSerializer.instance;
    }

    def "Test Fixed Numbers"() {
        setup:
        def list = session.withTransaction { t ->
            return t.prepared('select * from fixed_numbers order by id asc;', [], { it.toList(); }); };

        expect:
        list[0] == [1, true, 42, 420, 4200, 3.14f, 3.14159265d, new Money(100_00)];
        list[1] == [2, false, 43, 430, 4300, 2.71f, 2.71828182d, new Money(37_500_00)];
    }

    def "Test Fixed Numbers Write"() {
        when:
        List toInsert = [ true, (short) 20, 200, 2_000_000_000_000L, Float.MAX_VALUE, Double.MAX_VALUE, new Money(123456789) ];
        int inserted = session.withTransaction { t ->
            return t.prepared('insert into fixed_numbers ' +
                              '(my_boolean, my_smallint, my_int, my_long, my_real, my_double, my_money) ' +
                              'values ($1, $2, $3, $4, $5, $6, $7);', toInsert); };
        then:
        inserted == 1;

        when:
        List list = session.withTransaction { t ->
            t.prepared('select * from fixed_numbers where id = (select max(id) from fixed_numbers);',
                       [], { it.toList(); }); }[0];
        
        then:
        list[0] > 2;
        list.size() == toInsert.size() + 1;
        toInsert == list[1..<list.size()];

        when:
        int deleted = session.withTransaction { t ->
            t.prepared('delete from fixed_numbers where id = $1;', [ list[0] ]); };

        then:
        deleted == 1;
    }

    def "Test All Dates"() {
        setup:
        def list = session.withTransaction { t ->  
            return t.prepared('select * from all_dates order by id asc;', [], { it.toList(); }); }[0];

        expect:
        list[0] == 1;
        list[1] == LocalDate.of(1999, 1, 8);
        list[2] == LocalTime.of(4, 5, 6, 789_000_000);
        list[3] == OffsetTime.of(4, 5, 6, 789_000_000, ZoneOffset.of('-06:00:00'));
        list[4] == LocalDateTime.of(1999, 1, 8, 4, 5, 6, 789_000_000);
        list[5].isEqual(OffsetDateTime.of(1999, 1, 8, 4, 5, 6, 789_000_000, ZoneOffset.of('-06:00:00')));
    }

    def "Test All Dates Write"() {
        when:
        List toInsert = [ LocalDate.of(2010, 10, 31),
                          LocalTime.of(13, 25, 17, 900_000_000),
                          OffsetTime.of(13, 25, 17, 900_000_000, ZoneOffset.of('-04:00:00')),
                          LocalDateTime.of(2010, 10, 31, 13, 25, 17, 900_000_000),
                          OffsetDateTime.of(2010, 10, 31, 13, 25, 17, 900_000_000, ZoneOffset.of('-04:00:00')) ];
        int inserted = session.withTransaction { t ->
            return t.prepared('insert into all_dates (my_date, my_time, my_time_tz, my_timestamp, my_timestamp_tz) ' +
                              'values ($1, $2, $3, $4, $5);', toInsert); };
        then:
        inserted == 1;

        when:
        List list = session.withTransaction { t ->
            t.prepared('select * from all_dates where id = (select max(id) from all_dates);',
                       [], { it.toList(); }); }[0];
        
        then:
        list.size() == 6;
        toInsert[0] == list[1];
        toInsert[1] == list[2];
        toInsert[2] == list[3];
        toInsert[3] == list[4];
        toInsert[4].toInstant() == list[5].toInstant();

        when:
        int deleted = session.withTransaction { t ->
            t.prepared('delete from all_dates where id = $1;', [ list[0] ]); };

        then:
        deleted == 1;
    }

    def "Test Bytes"() {
        when:
        def list = session.withTransaction { t ->  
            return t.prepared('select * from binary_fields order by id asc;', [], { it.toList(); }); }[0];
        then:
        list.size() == 2;
        list[1] == [ 0xde, 0xad, 0xbe, 0xef ] as byte[];

        when:
        def toInsert = [ [ 1, 2, 3, 4, 5, 6, 7, 8 ] as byte[] ];
        int inserted = session.withTransaction { t ->
            return t.prepared('insert into binary_fields (my_bytes) values ($1) returning id;',
                              toInsert, { r -> r.single(); } ); }[0];
        then:
        inserted > 1;

        when:
        List newList = session.withTransaction { t ->
            t.prepared('select my_bytes from binary_fields where id = $1;', [inserted]) { r -> r.single(); }; };
        then:
        newList == toInsert;

        when:
        int deleted = session.withTransaction { t ->
            t.prepared('delete from binary_fields where id = $1;', [inserted]); };
        then:
        deleted == 1;
    }

    @Ignore
    def "Test Simple Automatic Serialization"() {
        setup:
        def sql = 'select * from items;';
        def task = SimpleTask.query(sql, { Row row -> row.toArray(); }).toCompletable();
        def list = session.execute(task).get();
        println(list);
        
        expect:
        list.size() == 2;
        list[0][1] instanceof Integer;
        list[0][1] == 1;
        list[0][2] instanceof String;
        list[0][2] == 'one';
    }

    @Ignore
    def "Test All Types Automatic Serialization"() {
        setup:
        def sql = 'select * from all_types;';
        def task = SimpleTask.query(sql, { Row row -> row.toMap(); }).toCompletable();
        def map = session.execute(task).get()[0];
        println(map);

        expect:
        map.size() == 18;
        map.my_bytes[0] == (byte) 0xde;
        map.my_bytes[1] == (byte) 0xad;
        map.my_bytes[2] == (byte) 0xbe;
        map.my_bytes[3] == (byte) 0xef;
    }
}
