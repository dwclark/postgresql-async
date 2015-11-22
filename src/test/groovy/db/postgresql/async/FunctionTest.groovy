package db.postgresql.async

import db.postgresql.async.tasks.TransactionTask;
import spock.lang.*

import static db.postgresql.async.Task.*
import static db.postgresql.async.Transaction.*;

class FunctionTest extends Specification {

    @Shared Session session;
    Concurrency concurrency = new Concurrency();
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    def "Simple Function"() {
        setup:
        Task t = prepared('select select_numerals();', [], { Row r -> r.toMap(); });
        CompletableTask ct = single(concurrency, t);
        session.execute(ct).get();
    }
}
