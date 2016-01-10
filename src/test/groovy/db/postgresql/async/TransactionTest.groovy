package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.types.*;
import db.postgresql.async.*;
import db.postgresql.async.enums.*;
import java.time.*;
import static db.postgresql.async.Task.*;
import static db.postgresql.async.Task.Prepared.*;

class TransactionTest extends Specification {

    private static Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    def "Minimal Transaction"() {
        when:
        def result = session(transaction([])
                             << { a ->
                                 query('select * from all_types;', NO_ARGS, a) { l, r ->
                                     l << r.toList();
                                 }
                             }).get();
        then:
        result.size() == 1;
        println(result[0]);

        when:
        def result2 = session(transaction([])
                              << { a ->
                                  query('select * from all_types;', NO_ARGS, a) { l, r ->
                                      l << r.toList();
                                  }
                              }).get();
        then:
        result2.size() == 1;
    }

    def "Same Query In Transaction Twice"() {
        when:
        def result = session(transaction([])
                             << { a ->
                                 query('select * from all_types;', NO_ARGS, a) { l, r ->
                                     l << r.toList();
                                 }
                             }
                             << { a ->
                                 query('select * from all_types;', NO_ARGS, a) { l, r ->
                                     l << r.toList();
                                 }
                             }).get();
        then:
        result.size() == 2;
        result[0] == result[1];
    }

    def "Test Transactional CRUD"() {
        setup:
        def r = session(transaction([])
                        << { accum ->
                            accum[0] = 0;
                            query('select * from numerals;', NO_ARGS, accum) { list, row ->
                                list[0]++;
                                return list;
                            }
                        }
                        << { accum ->
                            execute('insert into numerals (arabic, roman) values ($1,$2);', [ 21, 'xxi' ]) {
                                count -> accum[1] = count;
                            }
                        }
                        << { accum ->
                            accum[2] = 0;
                            query('select * from numerals;', NO_ARGS, accum) { list, row ->
                                list[2]++;
                                return accum;
                            }
                        }
                        << { accum ->
                            execute('delete from numerals where id > $1;', [ 20 ]);
                        }).get();
        expect:
        r[0] == 20;
        r[1] == 1;
        r[2] == 21;
    }

    def "Test Rollback"() {
        setup:
        def findSize = query('select count(*) from numerals', NO_ARGS, { r -> r.single(); });
        def size = session(findSize).get();

        when:
        session(transaction(null)
                << { execute('insert into numerals (arabic, roman) values ($1,$2);', [ 21, 'xxi' ]) }
                << { execute('insert into numerals (arabic, roman) values ($1,$2);', [ 22, 'xxii' ]) }
                << { execute('insert into numerals (arabic, roman) values ($1,$2);', [ 22, 'xxiii' ]) }
                << { rollback() }).get();
        def newSize = session(query('select count(*) from numerals', NO_ARGS, { r -> r.single(); })).get();

        then:
        newSize == size;
    }
}
