package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.tasks.*;
import static db.postgresql.async.tasks.SimpleTask.*;
import static db.postgresql.async.Transaction.*;

class TransactionTest extends Specification {

    @Shared Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    def "Define Simple Transactions"() {
        setup:
        Concurrency concurrency = new Concurrency();
        Transaction one = single(concurrency,
                                 forExecute("insert into items (id, description) values (44, 'forty-four');"));

        Transaction multi = multiple(concurrency,
                                     forExecute("insert into items (id, description) values (44, 'fourty-four');"),
                                     forExecute("update items set description = 'forty-four' where id 44;"));
    }

}
