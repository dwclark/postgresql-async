package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.enums.*;

class Helper {

    static final String host = '127.0.0.1';
    static final int port = 5432;
    static final String database = 'testdb';
    static final enumMap = [ (DaysOfWeek): 'public.days_of_week', (Moods): 'public.moods' ];

    static basic() {
        new SessionInfo.Builder().with {
            host '127.0.0.1'
            port 5432
            database 'testdb'
            channels 1, 5
            enumMapping DaysOfWeek, 'public.days_of_week'
            enumMapping Moods, 'public.moods'
            return it
        }
    }

    public static Session noAuth() {
         basic().with {
            user 'noauth'
            toSession()
        }
    }

    public static Session clearAuth() {
        basic().with {
            user 'clearauth'
            password 'clearauth'
            toSession()
        }
    }

    public static Session md5Auth() {
        basic().with {
            user 'md5auth'
            password 'md5auth'
            toSession()
        }
    }

    public static Session noAuthLoadTypes() {
        Session session = noAuth();
        session.sessionInfo.registry.loadTypes(session);
        return session;
    }
}
