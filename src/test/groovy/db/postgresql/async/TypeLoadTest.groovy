package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
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
        list[0] == [1, true, 42, 420, 4200, 3.14f, 3.14159265d];
        list[1] == [2, false, 43, 430, 4300, 2.71f, 2.71828182d];
    }

    def "Test Fixed Numbers Write"() {
        when:
        List toInsert = [ true, (short) 20, 200, 2_000_000_000_000L, Float.MAX_VALUE, Double.MAX_VALUE ];
        int inserted = session.withTransaction { t ->
            return t.prepared('insert into fixed_numbers ' +
                              '(my_boolean, my_smallint, my_int, my_long, my_real, my_double) ' +
                              'values ($1, $2, $3, $4, $5, $6);', toInsert); };
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
    }

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
