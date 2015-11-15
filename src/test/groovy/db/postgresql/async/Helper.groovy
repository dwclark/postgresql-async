package db.postgresql.async;

import spock.lang.*;

class Helper {

    static final String host = '127.0.0.1';
    static final int port = 5432;
    static final String database = 'testdb';
    static final SessionInfo.Builder builder = new SessionInfo.Builder().host(host).port(port).database(database);

    public static Session noAuth() {
        return new Session(builder.user('noauth').build(), null);
    }
}
