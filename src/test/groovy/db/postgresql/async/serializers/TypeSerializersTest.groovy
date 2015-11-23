package db.postgresql.async.serializers;

import spock.lang.*;
import db.postgresql.async.*;
import db.postgresql.async.tasks.*;
import java.net.*;

class TypeSerializersTest extends Specification {

    @Shared Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    def "Test Network Types"() {
        setup:
        def task = Task.simple('select * from network_types order by id asc;', { Row r -> r.toMap() }).toCompletable();
        def rows = session.execute(task).get();

        expect:
        rows.size() == 3;
        rows[0].my_inet.address instanceof Inet4Address;
        rows[1].my_inet.address instanceof Inet4Address;
        rows[2].my_inet.address instanceof Inet6Address;
    }
}
