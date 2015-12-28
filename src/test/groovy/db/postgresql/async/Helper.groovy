package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.enums.*;

class Helper {

    static final String host = '127.0.0.1';
    static final int port = 5432;
    static final String database = 'testdb';
    static final enumMap = [ (DaysOfWeek): 'public.days_of_week', (Moods): 'public.moods' ];
    
    static addEnums(builder) {
        builder.enumMapping(DaysOfWeek, 'public.days_of_week');
        builder.enumMapping(Moods, 'public.moods');
    }

    public static Session noAuth() {
        SessionInfo.Builder builder = new SessionInfo.Builder().host(host).port(port).database(database).channels(1, 5);
        addEnums(builder);
        return new Session(builder.user('noauth').build(), null);
    }

    public static Session clearAuth() {
        SessionInfo.Builder builder = new SessionInfo.Builder().host(host).port(port).database(database);
        addEnums(builder);
        return new Session(builder.user('clearauth').password('clearauth').build(), null);
    }

    public static Session md5Auth() {
        SessionInfo.Builder builder = new SessionInfo.Builder().host(host).port(port).database(database);
        addEnums(builder);
        return new Session(builder.user('md5auth').password('md5auth').build(), null);
    }

    public static Session noAuthLoadTypes() {
        Session session = noAuth();
        session.sessionInfo.registry.loadTypes(session);
        return session;
    }
}
