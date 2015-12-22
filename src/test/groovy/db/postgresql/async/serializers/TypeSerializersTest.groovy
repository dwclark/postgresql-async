package db.postgresql.async.serializers;

import spock.lang.*;
import db.postgresql.async.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.enums.*;
import java.net.*;

@Ignore
class TypeSerializersTest extends Specification {

    @Shared Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    @Ignore
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

    @Ignore
    def "Test Xml and Json Types"() {
        setup:
        def task = Task.simple('select * from json_and_xml;', { Row r -> r.toMap(); }).toCompletable();
        def rows = session.execute(task).get();

        expect:
        rows.size() == 1;
        rows[0].my_xml instanceof String;
        rows[0].my_json instanceof String;
        rows[0].my_json_b instanceof String;
    }

}
