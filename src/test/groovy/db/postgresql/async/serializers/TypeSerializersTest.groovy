package db.postgresql.async.serializers;

import spock.lang.*;
import db.postgresql.async.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.enums.*;
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

    def "Test Enum Types"() {
        setup:
        def task = Task.simple('select * from my_moods order by id;', { Row r -> r.toMap(); }).toCompletable();
        def rows = session.execute(task).get();

        expect:
        rows.size() == 2
        rows[0].my_day_of_the_week == DaysOfWeek.MONDAY;
        rows[0].my_mood == Moods.MAD;
        rows[1].my_day_of_the_week == DaysOfWeek.FRIDAY;
        rows[1].my_mood == Moods.HAPPY;
    }
}
