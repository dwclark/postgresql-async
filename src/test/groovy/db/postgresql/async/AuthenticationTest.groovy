package db.postgresql.async;

import spock.lang.*;
import java.nio.*;
import java.nio.charset.*;
import db.postgresql.async.messages.*;

class AuthenticationTest extends Specification {

    final String host = '127.0.0.1';
    final int port = 5432;
    final String database = 'testdb';
    final SessionInfo.Builder builder = new SessionInfo.Builder().host(host).port(port).database(database);
    final FrontEndMessage feMessage = new FrontEndMessage(Charset.forName('UTF-8'));

    def "No Authentication"() {
        when:
        def info = builder.user('noauth').build();
        Session session = new Session(info, null);

        then:
        session.ioCount == 1;

        when:
        session.shutdown();

        then:
        session.ioCount == 0;
    }

    def "Clear Text Password"() {
        setup:
        def info = builder.user('clearauth').password('clearauth').build();
        Session session = new Session(info, null);

        expect:
        session.ioCount == 1;
        
        cleanup:
        session.shutdown();
    }

    def "MD5 Password"() {
        setup:
        def info = builder.user('md5auth').password('md5auth').build();
        Session session = new Session(info, null);

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
