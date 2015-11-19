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

    def "Test Execute Simple"() {
        setup:
        String sql = "select * from all_types;";
        ExecuteTask<List<Map>> task = forQuery(sql, [] as Object[], { Row r -> r.toMap(); });
        CompletableTask ct = task.toCompletable();
        List<Map> output = session.execute(ct).get();

        task = forQuery(sql, [] as Object[], { Row r -> r.toMap(); });
        ct = task.toCompletable();
        List<Map> output2 = session.execute(ct).get();
    }

    def "Test Query With Args"() {
        setup:
        String sql = 'select * from items where id = $1;';
        ExecuteTask<List<Map>> task = forQuery(sql, [2] as Object[], { Row r -> r.toMap(); });
        CompletableTask ct = task.toCompletable();
        List<Map> output = session.execute(ct).get();

        expect:
        println(output);
        output[0].id == 2;
        output[0].description == 'two';
    }
}

