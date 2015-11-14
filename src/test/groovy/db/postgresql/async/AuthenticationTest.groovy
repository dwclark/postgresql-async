package db.postgresql.async;

import spock.lang.*;

class AuthenticationTest extends Specification {

    final String host = '127.0.0.1';
    final int port = 5432;
    final String database = 'testdb';
    SessionInfo.Builder builder;
    
    def setup() {
        builder = new SessionInfo.Builder().host(host).port(port).database(database);
    }
    

    def "No Authentication"() {
        setup:
        def info = builder.user('noauth').build();
        Session session = new Session(info, null);
    }
}
