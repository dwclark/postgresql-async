package db.postgresql.async

import db.postgresql.async.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import spock.lang.*
import static db.postgresql.async.Direction.*;
import static db.postgresql.async.Task.*
import static db.postgresql.async.Task.Prepared.*;
import static db.postgresql.async.Transaction.*;

class FunctionTest extends Specification {

    private static Session session;
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

    def "Simple Function Return Arrays"() {
        setup:
        def task = applyRows('select * from select_numerals();', NO_ARGS, { r -> r.toList(); });
        def contents = session(task).get().head();
        def kvList = contents[1].collect { it.keysValues(); };
        
        expect:
        contents;
        contents.size() == 2;
        kvList.each { println(it); }
    }

    def "Simple Function With Multiple Cursors"() {
        setup:
        def e =
            session(transaction(new Expando())
                    << { e ->
                        e.cursors = [];
                        println("Before returning acceptRows");
                        acceptRows('select multiple_cursors();', NO_ARGS) { row ->
                            println("Inside acceptRows");
                            e.cursors << row.single();
                        }
                    }
                    << { e ->
                        e.numerals = [];
                        e.cursors[0].acceptRows { row -> e.numerals << row.toMap(); };
                    }
                    << { e ->
                        e.items = [];
                        e.cursors[1].acceptRows { row -> e.items << row.toMap(); };
                    }
                    << { e ->
                        e.allTypes = [];
                        e.cursors[2].acceptRows { row -> e.allTypes << row.toMap(); };
                    }).get();
        expect:
        e.cursors.size() == 3;
        e.numerals.size() == 20;
        e.items.size() == 2;
        e.allTypes.size() == 1;
    }
}
