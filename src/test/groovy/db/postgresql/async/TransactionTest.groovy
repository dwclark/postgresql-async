package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.tasks.*;
import static db.postgresql.async.tasks.SimpleTask.*;
import static db.postgresql.async.tasks.Transaction.*;

class TransactionTest extends Specification {

    @Shared Session session;
    Concurrency concurrency = new Concurrency();
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    def "Define Simple Transactions"() {
        setup:
        Transaction one = single(concurrency,
                                 execute("insert into items (id, description) values (44, 'forty-four');"));
        
        Transaction multi = multiple(concurrency,
                                     execute("insert into items (id, description) values (44, 'fourty-four');"),
                                     execute("update items set description = 'forty-four' where id 44;"));
    }
    
    def "Run Simple Transaction"() {
        setup:
        def list = session.execute(single(concurrency,
                                          query("select * from all_types;",
                                                { Row r -> r.toMap(); }))).get();
        expect:
        list.size() == 1;
    }

}
