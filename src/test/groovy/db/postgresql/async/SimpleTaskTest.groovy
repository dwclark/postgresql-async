package db.postgresql.async;

import spock.lang.*;
import static db.postgresql.async.QueryPart.*;
import db.postgresql.async.tasks.*
import static db.postgresql.async.Task.Simple.*;

class SimpleTaskTest extends Specification {

    public static Session session;

    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }
    
    def cleanupSpec() {
        session.shutdown();
    }

    def "Test Insert/Update/Delete/Select"() {
        when:
        int inserted = session(count("insert into items (id, description) values (3, 'three');")).get();
        
        then:
        inserted == 1;

        when:
        int updated = session(count("update items set description = 'iii' where id = 3;")).get();

        then:
        updated == 1;

        when:
        def func = { List<Map> accum, Row row -> accum << row.toMap(); };
        def map = session(applyRows('select * from items where id = 3;', [], func)).get()[0];
        
        then:
        map.description == 'iii';

        when:
        int deleted = session(count('delete from items where id = 3;')).get();

        then:
        deleted == 1;
    }

    public MapEntry multiSelect(int notUsed) {
        def func =  { List<Map> accum, Row r -> accum << r.toMap(); }
        def part1 = partList('select * from items where id < 3;', func);
        def part2 = partList('select * from all_types;', func);
        def task = bulkExecute([part1, part2]);

        def expect = { results ->
            results.size() == 2 && results[0].size() == 2 &&  results[1].size() == 1;
        };

        return new MapEntry(task, expect);
    }

    def "Test Multi Select"() {
        setup:
        def entry = multiSelect(10);
        def results = session(entry.key).get();

        expect:
        entry.value.call(results);
    }

    public MapEntry multiPhase(int index) {
        def func = { List<Map> accum, Row row -> accum << row.toMap(); };
        def parts = [ partCount("insert into items (id, description) values (${index}, 'number ${index}');"),
                      partCount("update items set description = 'iii' where id = ${index};"),
                      partList("select * from items where id < 3;", func),
                      partCount("delete from items where id = ${index};") ];
        def task = bulkExecute(parts);

        def expect = { results ->
            (results.size() == 4 && results[0] == 1 && results[1] == 1 &&
             results[2].size() == 2 && results[3] == 1);
        };

        return new MapEntry(task, expect);
    }

    def "Test Multi Phase"() {
        setup:
        def entry = multiPhase(20);
        def results = session(entry.key).get();

        expect:
        entry.value.call(results);
    }

    public MapEntry singleTask(int notUsed) {
        def func = { Row row -> row.toMap(); };
        def task = applyRows('select * from items where id < 3;', func);
        def expect = { results -> results.size() == 2; };

        return new MapEntry(task, expect);
    }

    def "Test Single Task"() {
        setup:
        def entry = singleTask(1);
        def results = session(entry.key).get();

        expect:
        entry.value.call(results);
    }

    static final INTERMIX_COUNT = 1_000;
    
    public void simpleIntermix(int myIndex) {
        def rand = new Random();
        def closures = [ this.&singleTask, this.&multiPhase, this.&multiSelect ];
        (0..<INTERMIX_COUNT).each { num ->
            int closureIndex = rand.nextInt(3);
            def closure = closures[closureIndex];
            def entry = closure.call(myIndex);
            def results = session.call(entry.key).get();
            //println("Thread: ${Thread.currentThread()}, Num: ${num}, Index: ${closureIndex}, results: ${results}");
            assert(entry.value.call(results));
        };
    }

    def "Test Simple Single Threaded Intermix"() {
        setup:
        simpleIntermix(25);
    }

    def "Test Simple Multi Threaded Intermix Max Pool"() {
        setup:
        final threadCount = 5;
        println("*************Test Simple Multi Threaded Intermix Max Pool ************");
        println("intermix count: ${INTERMIX_COUNT}, threads: ${threadCount}");
        println("Starting time: ${new Date()}, ");
        def list = (0..<threadCount).collect { index -> Thread.start(this.&simpleIntermix.curry(index + 10)); };
        list.each { it.join(); };
        println("Ending time: ${new Date()}");
    }

    def "Test Simple Multi Threaded Intermix Overmaxed Pool"() {
        setup:
        final threadCount = 10;
        println("*************Test Simple Multi Threaded Intermix Overmaxed Pool************");
        println("intermix count: ${INTERMIX_COUNT}, threads: ${threadCount}");
        println("Starting time: ${new Date()}");
        def list = (0..<threadCount).collect { index -> Thread.start(this.&simpleIntermix.curry(index + 10)); };
        list.each { it.join(); };
        println("Ending time: ${new Date()}");
    }
}
