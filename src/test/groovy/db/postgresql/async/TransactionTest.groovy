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
                            acceptRows('select * from all_types;') { row ->
                                e.original << row.toList();
                            }
                        }).get();
        then:
        e.original.size() == 1;
        e.original[0].size() == 18;
    }

    def "Same Query In Transaction Twice"() {
        when:
        def e = session(transaction(new Expando())
                        << { e ->
                            acceptRows('select * from all_types;') { row ->
                                e.first = row.toList();
                            }
                        }
                        << { e ->
                            acceptRows('select * from all_types;') { row ->
                                e.second = row.toList();
                            }
                        }).get();
        then:
        e.first == e.second;
    }

    def "Test Transactional CRUD"() {
        setup:
        def e = session(transaction(new Expando())
                        << { e ->
                            e.first = 0;
                            acceptRows('select * from numerals;') { row ->
                                e.first++;
                            }
                        }
                        << { e ->
                            count('insert into numerals (arabic, roman) values ($1,$2);', [ 21, 'xxi' ]) { num ->
                                e.count = num;
                            }
                        }
                        << { e ->
                            e.second = 0;
                            acceptRows('select * from numerals;') { row ->
                                e.second++;
                            }
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
        def size = session(acceptRows('select count(*) from numerals;') { row -> row.single(); }).get();
        
        when:
        session(transaction(null)
                << { count('insert into numerals (arabic, roman) values ($1,$2);', [ 21, 'xxi' ]) }
                << { count('insert into numerals (arabic, roman) values ($1,$2);', [ 22, 'xxii' ]) }
                << { count('insert into numerals (arabic, roman) values ($1,$2);', [ 22, 'xxiii' ]) }
                << { rollback(); }).get();
        
        def newSize = session(acceptRows('select count(*) from numerals;') { row -> row.single(); }).get();
        
        then:
        newSize == size;
    }
}
