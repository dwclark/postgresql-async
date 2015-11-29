package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.*;

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
