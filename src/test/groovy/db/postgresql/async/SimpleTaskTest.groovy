package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.tasks.*;
import static db.postgresql.async.tasks.SimpleTask.*;

class SimpleTaskTest extends Specification {

    @Shared Session session;

    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }
    
    def cleanupSpec() {
        session.shutdown();
    }

    def "Test Insert/Update/Delete/Select"() {
        when:
        int count = session.execute("insert into items (id, description) values (3, 'three');").get();

        then:
        count == 1;

        when:
        count = session.execute("update items set description = 'iii' where id = 3;").get();

        then:
        count == 1;

        when:
        def func = { List<Map> accum, Row row -> accum << row.toMap(); };
        def map = session.query('select * from items where id = 3;', [], func).get()[0];
        
        then:
        map;
        map.description == 'iii';

        when:
        count = session.execute('delete from items where id = 3;').get();

        then:
        count == 1;
    }

    public MapEntry multiSelect(int notUsed) {
        def func =  { List<Map> accum, Row r -> accum << r.toMap(); }
        def part1 = QueryPart.forList('select * from items where id < 3;', func);
        def part2 = QueryPart.forList('select * from all_types;', func);
        def task = SimpleTask.forMulti([ part1, part2]).toCompletable();

        def expect = { results ->
            results.size() == 2 && results[0].size() == 2 &&  results[1].size() == 1;
        };

        return new MapEntry(task, expect);
    }

    def "Test Multi Select"() {
        setup:
        def entry = multiSelect(10);
        def results = session.execute(entry.key).get();

        expect:
        entry.value.call(results);
    }

    public MapEntry multiPhase(int index) {
        def func = { List<Map> accum, Row row -> accum << row.toMap(); };
        def parts = [ QueryPart.forExecute("insert into items (id, description) values (${index}, 'number ${index}');"),
                      QueryPart.forExecute("update items set description = 'iii' where id = ${index};"),
                      QueryPart.forList("select * from items where id < 3;", func),
                      QueryPart.forExecute("delete from items where id = ${index};") ];
        def task = SimpleTask.forMulti(parts).toCompletable();

        def expect = { results ->
            (results.size() == 4 && results[0] == 1 && results[1] == 1 &&
             results[2].size() == 2 && results[3] == 1);
        };

        return new MapEntry(task, expect);
    }

    def "Test Multi Phase"() {
        setup:
        def entry = multiPhase(20);
        def results = session.execute(entry.key).get();

        expect:
        entry.value.call(results);
    }

    public MapEntry singleTask(int notUsed) {
        def func = { Row row -> row.toMap(); };
        def task = SimpleTask.forQuery('select * from items where id < 3;', func).toCompletable();
        def expect = { results -> results.size() == 2; };

        return new MapEntry(task, expect);
    }

    public void simpleIntermix(int myIndex) {
        def rand = new Random();
        def closures = [ this.&singleTask, this.&multiPhase, this.&multiSelect ];
        (0..<1000).each { num ->
            int closureIndex = rand.nextInt(3);
            def closure = closures[closureIndex];
            def entry = closure.call(myIndex);
            def results = session.execute(entry.key).get();
            println("Thread: ${Thread.currentThread()}, Num: ${num}, Index: ${closureIndex}, results: ${results}");
            assert(entry.value.call(results));
        };
    }

    def "Test Simple Single Threaded Intermix"() {
        setup:
        simpleIntermix(25);
    }

    def "Test Simple Multi Threaded Intermix Max Pool"() {
        setup:
        println("*************Test Simple Multi Threaded Intermix Max Pool************");
        println("Starting time: ${new Date()}");
        def list = (0..<5).collect { index -> Thread.start(this.&simpleIntermix.curry(index + 10)); };
        list.each { it.join(); };
        println("Ending time: ${new Date()}");
    }

    def "Test Simple Multi Threaded Intermix Overmaxed Pool"() {
        setup:
        println("*************Test Simple Multi Threaded Intermix Overmaxed Pool************");
        println("Starting time: ${new Date()}");
        def list = (0..<10).collect { index -> Thread.start(this.&simpleIntermix.curry(index + 10)); };
        list.each { it.join(); };
        println("Ending time: ${new Date()}");
    }
}
