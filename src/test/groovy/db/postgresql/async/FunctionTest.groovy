package db.postgresql.async

import db.postgresql.async.tasks.TransactionTask;
import spock.lang.*

import static db.postgresql.async.Task.*
import static db.postgresql.async.Transaction.*;
import db.postgresql.async.*;
import static db.postgresql.async.Direction.*;

class FunctionTest extends Specification {

    @Shared Session session;
    Concurrency concurrency = new Concurrency();
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    def "Simple Function With Cursor"() {
        setup:
        def cursorContents = [];
        session.withTransaction(concurrency) { Transaction t ->
            List results = t.prepared('select select_numerals();', [], { Row first -> first.toArray()[0]; });
            Cursor cursor = results[0];
            cursor.fetch(ALL, IGNORE_COUNT, { Row r -> cursorContents << r.toList(); }); };
        
        expect:
        cursorContents;
        println(cursorContents);
    }
}
