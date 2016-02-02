package db.postgresql.async;

import spock.lang.*;

class NotificationsTest extends Specification {

    private static Session session;
    
    def setupSpec() {
        session = Helper.noAuthNotifications();
    }
    
    def cleanupSpec() {
        session.shutdown();
    }

    def "Listen/Unlisten"() {
        when:
        def val = session.listen('channel1', { -> println("Do Something"); });//.get();
        sleep(100_000L)
        then:
        !val
        
        when:
        def uval = session.unlisten('channel1').get();

        then:
        !uval;
    }
}
