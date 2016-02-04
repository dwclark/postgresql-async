package db.postgresql.async;

import spock.lang.*;
import static db.postgresql.async.Task.Simple.*;
import java.util.concurrent.atomic.AtomicInteger;

class NotificationsTest extends Specification {

    private static Session session;
    
    def setupSpec() {
        session = Helper.noAuthNotifications();
    }
    
    def cleanupSpec() {
        session.shutdown();
    }

    def "Listen/Unlisten"() {
        setup:
        def counter = new AtomicInteger(0);
        
        when:
        def val = session.listen('channel1', { n -> counter.addAndGet(n.payload.toInteger()); } ).get();

        then:
        !val

        when:
        (0..100).each { num ->
            session.execute(noOutput("notify channel1, '${num}'")).get();
        }

        sleep(500L);
        def uval = session.unlisten('channel1').get();
        session.execute(noOutput("notify channel1, '100'")).get();
        sleep(300L);

        then:
        !uval;
        counter.get() == 5050;
    }

    def "Listen/Unlisten Multiple Channels"() {
        setup:
        def one = new AtomicInteger(0);
        def two = new AtomicInteger(0);
        def three = new AtomicInteger(0);
        session.listen('one', { n -> one.addAndGet(n.payload.toInteger()); } ).get();
        session.listen('two', { n -> two.addAndGet(n.payload.toInteger()); } ).get();
        session.listen('three', { n -> three.addAndGet(n.payload.toInteger()); } ).get();

        (1..50).each {
            session.execute(noOutput("notify one, '1'"));
            session.execute(noOutput("notify two, '2'"));
            session.execute(noOutput("notify three, '3'"));
        }

        sleep(200L);
        
        expect:
        one.get() == 50;
        two.get() == 100;
        three.get() == 150;
    }
}
