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

    def "Test Multi Select"() {
        setup:
        def func =  { List<Map> accum, Row r -> accum << r.toMap(); }
        def part1 = QueryPart.forList('select * from items;', func);
        def part2 = QueryPart.forList('select * from all_types;', func);
        def task = SimpleTask.forMulti([ part1, part2]).toCompletable();
        def results = session.execute(task).get();

        expect:
        results.size() == 2;
        results[0].size() == 2;
        results[1].size() == 1;
    }
}
