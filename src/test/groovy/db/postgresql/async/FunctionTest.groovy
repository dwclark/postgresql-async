package db.postgresql.async

import db.postgresql.async.tasks.TransactionTask;
import spock.lang.*

import static db.postgresql.async.Task.*
import static db.postgresql.async.Transaction.*;
import db.postgresql.async.*;
import static db.postgresql.async.Direction.*;
import java.util.function.Function;
import java.util.function.BiFunction;

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
            List results = t.prepared('select select_numerals();', [], { Row first -> first.single(Cursor); } as Function);
            Cursor cursor = results[0];
            cursor.fetch(ALL, IGNORE_COUNT, { Row r -> cursorContents << r.toList(); }); };
        
        expect:
        cursorContents;
        cursorContents.size() == 20;
    }

    def "Simple Function With Multiple Cursors"() {
        setup:
        def numerals = [];
        def items = [];
        def allTypes = [];
        session.withTransaction(concurrency) { Transaction t ->
            List results = t.prepared('select multiple_cursors();', [], { Row r -> r.single(Cursor); });
            results[0].fetch(ALL, IGNORE_COUNT, { numerals << it.toMap(); });
            results[1].fetch(ALL, IGNORE_COUNT, { items << it.toMap(); });
            results[2].fetch(ALL, IGNORE_COUNT, { allTypes << it.toMap(); }); };

        expect:
        numerals;
        items.size() == 2;
        allTypes.size() == 1;
    }
}
