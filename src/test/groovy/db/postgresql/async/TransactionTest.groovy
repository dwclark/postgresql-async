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
import java.util.function.Function;

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
        def e = session(transaction(new Expando())
                        << { e ->
                            e.original = [];
                            accept('select * from all_types;',
                                   NO_ARGS,
                                   { r -> e.original << r.toList(); });
                        }).get();
        then:
        e.original.size() == 1;
        e.original[0].size() == 18;
    }

    def "Same Query In Transaction Twice"() {
        when:
        def e = session(transaction(new Expando())
                        << { e ->
                            accept('select * from all_types;',
                                   NO_ARGS,
                                   { r -> e.first = r.toList() });
                        }
                        << { e ->
                            accept('select * from all_types;',
                                   NO_ARGS,
                                   { r -> e.second = r.toList(); });
                        }).get();
        then:
        e.first == e.second;
    }

    def "Test Transactional CRUD"() {
        setup:
        def e = session(transaction(new Expando())
                        << { e ->
                            e.first = 0;
                            accept('select * from numerals;',
                                   NO_ARGS,
                                   { r -> e.first++; });
                        }
                        << { e ->
                            acceptCount('insert into numerals (arabic, roman) values ($1,$2);',
                                        [ 21, 'xxi' ],
                                        { i -> e.count = i; });
                        }
                        << { e ->
                            e.second = 0;
                            accept('select * from numerals;',
                                   NO_ARGS,
                                   { r -> e.second++; });
                        }
                        << { e ->
                            count('delete from numerals where id > $1;', [ 20 ]);
                        }).get();
        expect:
        e.first == 20;
        e.count == 1;
        e.second == 21;
    }

    def "Test Rollback"() {
        setup:
        def size = session(accept('select count(*) from numerals;',
                                  NO_ARGS,
                                  { r -> r.single(); })).get();
        
        when:
        session(transaction(null)
                << { count('insert into numerals (arabic, roman) values ($1,$2);', [ 21, 'xxi' ]) }
                << { count('insert into numerals (arabic, roman) values ($1,$2);', [ 22, 'xxii' ]) }
                << { count('insert into numerals (arabic, roman) values ($1,$2);', [ 22, 'xxiii' ]) }
                << { rollback(); }).get();
        
        def newSize = session(accept('select count(*) from numerals;',
                                     NO_ARGS,
                                     { r -> r.single(); })).get();
        
        then:
        newSize == size;
    }
}
