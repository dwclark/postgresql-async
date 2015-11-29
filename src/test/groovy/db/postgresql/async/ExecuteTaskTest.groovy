package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.*;
import db.postgresql.async.pginfo.*;

@Ignore
class ExecuteTaskTest extends Specification {

    @Shared Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    @Ignore
    def "Test Query Simple"() {
        setup:
        String sql = "select * from all_types;";
        CompletableTask ct = Task.prepared(sql, [], { Row r -> r.toMap(); }).toCompletable();
        List<Map> output = session.execute(ct).get();

        ct = Task.prepared(sql, [], { Row r -> r.toMap(); }).toCompletable();
        List<Map> output2 = session.execute(ct).get();
    }

    @Ignore
    def "Test Query With Args"() {
        setup:
        String sql = 'select * from items where id = $1;';
        CompletableTask ct = Task.prepared(sql, [2], { Row r -> r.toMap(); }).toCompletable();
        List<Map> output = session.execute(ct).get();

        expect:
        println(output);
        output[0].id == 2;
        output[0].description == 'two';
    }

    @Ignore
    def "Test Multiple Executions"() {
        setup:
        (0..10).each { num ->
            [ Task.prepared("select * from all_types;", [], { Row r -> r.toMap(); }).toCompletable(),
              Task.prepared('select * from items where id = $1;', [2], { Row r -> r.toMap(); }).toCompletable() ].each { ct ->
                  println("Execution ${num}");
                  assert(session.execute(ct).get()); }; };
    }
}

