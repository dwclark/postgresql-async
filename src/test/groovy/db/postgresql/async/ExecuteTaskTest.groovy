package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.*;
import db.postgresql.async.pginfo.*;
import static db.postgresql.async.tasks.ExecuteTask.*;

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
        CompletableTask ct = query(sql, [] as Object[], { Row r -> r.toMap(); }).toCompletable();
        List<Map> output = session.execute(ct).get();

        ct = query(sql, [] as Object[], { Row r -> r.toMap(); }).toCompletable();
        List<Map> output2 = session.execute(ct).get();
    }

    def "Test Query With Args"() {
        setup:
        String sql = 'select * from items where id = $1;';
        CompletableTask ct = query(sql, [2] as Object[], { Row r -> r.toMap(); }).toCompletable();
        List<Map> output = session.execute(ct).get();

        expect:
        println(output);
        output[0].id == 2;
        output[0].description == 'two';
    }

    def "Test Multiple Executions"() {
        setup:
        (0..10).each { num ->
            [ query("select * from all_types;", [] as Object[], { Row r -> r.toMap(); }).toCompletable(),
              query('select * from items where id = $1;', [2] as Object[], { Row r -> r.toMap(); }).toCompletable() ].each { ct ->
                  println("Execution ${num}");
                  assert(session.execute(ct).get()); }; };
    }
}

