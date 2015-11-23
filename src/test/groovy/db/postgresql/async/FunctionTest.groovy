package db.postgresql.async

import db.postgresql.async.tasks.TransactionTask;
import spock.lang.*

import static db.postgresql.async.Task.*
import static db.postgresql.async.Transaction.*;
import db.postgresql.async.*;
import static db.postgresql.async.Direction.*;
import java.util.function.Consumer;
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

    final static extractCursor = { it.single(Cursor); };
    final static extractList = { it.toList(); };
    final static extractMap = { it.toMap(); };

    def "Simple Function With Cursor"() {
        setup:
        def contents = session.withTransaction(concurrency) {
            Transaction t ->
                t.prepared('select select_numerals();', [], extractCursor).head().with {
                    toList(ALL, extractList); }; };
        
        expect:
        contents;
        contents.size() == 20;
    }

    def "Simple Function With Multiple Cursors"() {
        setup:
        def (numerals, items, allTypes) = session.withTransaction(concurrency) {
            Transaction t ->
                t.prepared('select multiple_cursors();', [], extractCursor).collect {
                    Cursor cursor -> cursor.toList(ALL, extractMap); }; };
        
        expect:
        numerals.size() == 20;
        items.size() == 2;
        allTypes.size() == 1;
    }
}
