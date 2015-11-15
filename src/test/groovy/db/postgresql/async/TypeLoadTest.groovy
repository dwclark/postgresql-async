package db.postgresql.async;

import spock.lang.*;

class TypeLoadTest extends Specification {

    Session session;
    
    def setup() {
        session = Helper.noAuth();
    }

    def cleanup() {
        session.shutdown();
    }

    def "Test Load Attributes"() {
        setup:
        def list = session.loadAttributes();

        expect:
        list;
    }
}
