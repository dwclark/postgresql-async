package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.*;
import db.postgresql.async.pginfo.*;
import static db.postgresql.async.Task.Prepared.*;

class ExecuteTaskTest extends Specification {

    @Shared Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    def "Test Query Simple"() {
        setup:
        String sql = "select * from all_types;";
        CompletableTask ct = applyRows(sql, NO_ARGS, { Row r -> r.toMap(); }).toCompletable();
        List<Map> output = session.execute(ct).get();

        ct = applyRows(sql, [], { Row r -> r.toMap(); }).toCompletable();
        List<Map> output2 = session.execute(ct).get();
    }

    def "Test Query With Args"() {
        setup:
        String sql = 'select * from items where id = $1;';
        CompletableTask ct = applyRows(sql, [2], { Row r -> r.toMap(); }).toCompletable();
        List<Map> output = session.execute(ct).get();

        expect:
        println(output);
        output[0].id == 2;
        output[0].description == 'two';
    }
    
    def "Test Multiple Executions"() {
        setup:
        (0..10).each { num ->
            [ applyRows("select * from all_types;", NO_ARGS, { Row r -> r.toMap(); }).toCompletable(),
              applyRows('select * from items where id = $1;', [2], { Row r -> r.toMap(); }).toCompletable() ].each { ct ->
                  //println("Execution ${num}");
                  assert(session.execute(ct).get()); }; };
    }
}

