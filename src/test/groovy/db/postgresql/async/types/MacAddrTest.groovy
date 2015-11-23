package db.postgresql.async.types;

import spock.lang.*;

class MacAddrTest extends Specification {

    def "Test Parsing"() {
        setup:
        final MacAddr one = MacAddr.fromString('08:00:2b:01:02:03');
        final MacAddr two = MacAddr.fromString('08-00-2b-01-02-03');

        expect:
        one == two;
    }

    def "Test Constructor"() {
        setup:
        final MacAddr one = new MacAddr([ 0xa, 0xb, 0xc, 0xd, 0xe, 0xf ] as byte[]);
        final MacAddr two = MacAddr.fromString('0a:0b:0c:0d:0e:0f');

        expect:
        one == two;
        one.hashCode() == two.hashCode();
        one.toString() == two.toString();
    }
}
