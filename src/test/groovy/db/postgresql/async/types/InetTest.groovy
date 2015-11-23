package db.postgresql.async.types;

import spock.lang.*;
import java.net.*;

class InetTest extends Specification {

    def "Test Basics IP 4"() {
        setup:
        def addr1 = InetAddress.getByName("192.168.1.1");
        def inet1 = new Inet(addr1);
        def inet2 = new Inet(addr1, (short) 32);
        def inet3 = new Inet(InetAddress.getByName('10.10.24.27'), (short) 16);

        expect:
        inet1.toString() == '192.168.1.1/32';
        inet1.toString() == inet2.toString();
        inet1 == inet2;
        inet1.hashCode() == inet2.hashCode();
        inet3.toString() == '10.10.24.27/16';
        inet1 != inet3;
        inet3 == Inet.fromString('10.10.24.27/16');
    }

    def "Test Basics IP 6"() {
        setup:
        def addr1 = InetAddress.getByName("2001:4f8:3:ba:2e0:81ff:fe22:d1f1");
        def inet1 = new Inet(addr1);
        def inet2 = new Inet(addr1, (short) 128);
        def inet3 = new Inet(InetAddress.getByName('2001:4f8:3:ba:0:0:0:0'), (short) 120);

        expect:
        inet1.toString() == '2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128';
        inet1.toString() == inet2.toString();
        inet1 == inet2;
        inet1.hashCode() == inet2.hashCode();
        inet3.toString() == '2001:4f8:3:ba:0:0:0:0/120';
        inet1 != inet3;
        inet3 == Inet.fromString('2001:4f8:3:ba:0:0:0:0/120');
    }
}
