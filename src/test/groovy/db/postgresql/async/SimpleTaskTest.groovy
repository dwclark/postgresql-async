package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.QueryPart;
import db.postgresql.async.tasks.*

@Ignore
class SimpleTaskTest extends Specification {

    @Shared Session session;

    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }
    
    def cleanupSpec() {
        session.shutdown();
    }

    @Ignore
    def "Test Insert/Update/Delete/Select"() {
        when:
        final Task insert = Task.simple("insert into items (id, description) values (3, 'three');");
        int count = session.execute(insert.toCompletable()).get();

        then:
        count == 1;

        when:
        final Task update = Task.simple("update items set description = 'iii' where id = 3;");
        count = session.execute(update.toCompletable()).get();

        then:
        count == 1;

        when:
        def func = { List<Map> accum, Row row -> accum << row.toMap(); };
        final Task select = Task.simple('select * from items where id = 3;', [], func);
        def map = session.execute(select.toCompletable()).get()[0];
        
        then:
        map;
        map.description == 'iii';

        when:
        final Task delete = Task.simple('delete from items where id = 3;');
        count = session.execute(delete.toCompletable()).get();

        then:
        count == 1;
    }

    public MapEntry multiSelect(int notUsed) {
        def func =  { List<Map> accum, Row r -> accum << r.toMap(); }
        def part1 = QueryPart.forList('select * from items where id < 3;', func);
        def part2 = QueryPart.forList('select * from all_types;', func);
        def task = SimpleTask.multi([ part1, part2]).toCompletable();

        def expect = { results ->
            results.size() == 2 && results[0].size() == 2 &&  results[1].size() == 1;
        };

        return new MapEntry(task, expect);
    }

    @Ignore
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
        def task = SimpleTask.multi(parts).toCompletable();

        def expect = { results ->
            (results.size() == 4 && results[0] == 1 && results[1] == 1 &&
             results[2].size() == 2 && results[3] == 1);
        };

        return new MapEntry(task, expect);
    }

    @Ignore
    def "Test Multi Phase"() {
        setup:
        def entry = multiPhase(20);
        def results = session.execute(entry.key).get();

        expect:
        entry.value.call(results);
    }

    public MapEntry singleTask(int notUsed) {
        def func = { Row row -> row.toMap(); };
        def task = SimpleTask.query('select * from items where id < 3;', func).toCompletable();
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

    @Ignore
    def "Test Simple Single Threaded Intermix"() {
        setup:
        simpleIntermix(25);
    }

    @Ignore
    def "Test Simple Multi Threaded Intermix Max Pool"() {
        setup:
        println("*************Test Simple Multi Threaded Intermix Max Pool************");
        println("Starting time: ${new Date()}");
        def list = (0..<5).collect { index -> Thread.start(this.&simpleIntermix.curry(index + 10)); };
        list.each { it.join(); };
        println("Ending time: ${new Date()}");
    }

    @Ignore
    def "Test Simple Multi Threaded Intermix Overmaxed Pool"() {
        setup:
        println("*************Test Simple Multi Threaded Intermix Overmaxed Pool************");
        println("Starting time: ${new Date()}");
        def list = (0..<10).collect { index -> Thread.start(this.&simpleIntermix.curry(index + 10)); };
        list.each { it.join(); };
        println("Ending time: ${new Date()}");
    }
}
