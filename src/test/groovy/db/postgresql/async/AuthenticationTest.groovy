package db.postgresql.async;

import spock.lang.*;
import java.nio.*;
import java.nio.charset.*;
import db.postgresql.async.messages.*;

class AuthenticationTest extends Specification {

    final FrontEndMessage feMessage = new FrontEndMessage(Charset.forName('UTF-8'));

    def "No Authentication"() {
        when:
        Session session = Helper.noAuth();

        then:
        session.ioCount == 1;

        when:
        session.shutdown();

        then:
        session.ioCount == 0;
    }

    def "Clear Text Password"() {
        setup:
        Session session = Helper.clearAuth();

        expect:
        session.ioCount == 1;
        
        cleanup:
        session.shutdown();
    }

    def "MD5 Password"() {
        setup:
        Session session = Helper.md5Auth();

        expect:
        session.ioCount == 1;

        cleanup:
        session.shutdown();
    }

    def "SSL with MD5 Auth"() {
        setup:
        Session session = Helper.sslMd5Auth();

        expect:
        session.ioCount == 1;

        cleanup:
        session.shutdown();
    }

    def "MD5 Payload"() {
        setup:
        String user = 'david';
        String password = 'qwerty12345';
        Charset c = Charset.forName('UTF-8');
        ByteBuffer salt = ByteBuffer.wrap('aK*r'.getBytes(c));
        String payload = feMessage.md5Hash(user, password, salt);

        expect:
        payload == 'md53021e9c7078798c92184ad247ec9e6b6';
    }

    def "MD5 Payload 35 Chars"() {
        setup:
        String user = 'md5auth';
        String password = user;
        Charset c = Charset.forName('UTF-8');
        ByteBuffer salt = ByteBuffer.wrap([ 117, -75, -60, 31 ] as byte[]);
        String payload = feMessage.md5Hash(user, password, salt);

        expect:
        payload.length() == 35;
    }
}
