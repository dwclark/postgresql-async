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
        def list = session.loadTypes();

        expect:
        session.sessionInfo.registry.pgType(23);
        session.sessionInfo.registry.pgType(1043);
    }
}
