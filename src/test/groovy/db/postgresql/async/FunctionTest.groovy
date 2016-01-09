package db.postgresql.async

import db.postgresql.async.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import spock.lang.*
import static db.postgresql.async.Direction.*;
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

    final static extractCursor = { it.single(); };
    final static extractList = { it.toList(); };
    final static extractMap = { it.toMap(); };

    @Ignore
    def "Simple Function With Cursor"() {
        setup:
        def contents = session.withTransaction(concurrency) { t ->
            def results = t.prepared('select * from select_numerals();', []) { r -> r.toList(); }[0]; };
        def kvList = contents[1].collect { it.keysValues(); };
        
        expect:
        contents;
        contents.size() == 2;
        kvList.each { println(it); }
        
        /*def contents = session.withTransaction(concurrency) { t ->
            def intermediate = t.prepared('select select_numerals();', []) { r -> r.toList() };
            println(intermediate[0]);
            return intermediate[1].standard { r -> r.toList(); }; };
        
        expect:
        contents;
        contents.size() == 20;*/
    }

    //@Ignore
    def "Simple Function With Multiple Cursors"() {
        setup:
        def (numerals, items, allTypes) = session.withTransaction(concurrency) {
            Transaction t ->
                t.prepared('select multiple_cursors();', [], extractCursor).collect {
                    Cursor cursor -> cursor.standard { r -> r.toList(); }; }; };
        
        expect:
        println(numerals);
        numerals.size() == 20;
        println(items);
        items.size() == 2;
        println(allTypes);
        allTypes.size() == 1;
    }
}
