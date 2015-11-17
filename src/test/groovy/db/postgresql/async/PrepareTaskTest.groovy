package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.*;
import db.postgresql.async.pginfo.*;

class PrepareTaskTest extends Specification {

    @Shared Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    def "Test Prepare Task"() {
        when:
        String sql = "select * from all_types;";
        PgSessionCache cache = new PgSessionCache();
        PrepareTask pt = new PrepareTask(sql, cache);
        CompletableTask ct = pt.toCompletable();
        session.execute(ct).get();

        then:
        cache.size() == 1;
        cache.statement(sql) != null;
        cache.statement(sql).value == pt.id;
    }
}

