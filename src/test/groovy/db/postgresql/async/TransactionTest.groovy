package db.postgresql.async

import db.postgresql.async.tasks.TransactionTask;
import spock.lang.*

import static db.postgresql.async.tasks.SimpleTask.*;
import static db.postgresql.async.tasks.TransactionTask.*;

@Ignore
class TransactionTest extends Specification {

    @Shared Session session;
    Concurrency concurrency = new Concurrency();
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    @Ignore
    def "Define Simple Transactions"() {
        setup:
        TransactionTask one = single(concurrency,
                                 execute("insert into items (id, description) values (44, 'forty-four');"));
        
        TransactionTask multi = multiple(concurrency,
                                     execute("insert into items (id, description) values (44, 'fourty-four');"),
                                     execute("update items set description = 'forty-four' where id 44;"));
    }

    @Ignore
    def "Run Simple Transaction"() {
        setup:
        def list = session.execute(single(concurrency,
                                          query("select * from all_types;",
                                                { Row r -> r.toMap(); }))).get();
        expect:
        list.size() == 1;
    }

}
