package db.postgresql.async;

import java.util.function.Consumer;
import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.types.*;
import db.postgresql.async.*;
import db.postgresql.async.enums.*;
import java.time.*;
import java.nio.CharBuffer;
import static db.postgresql.async.Task.*;
import static db.postgresql.async.Task.Prepared.*;

class StreamingTest extends Specification {

    private static Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    def "Test Stream Nothing Streamable"() {
        setup:
        List all = session(applyFields('select * from fixed_numbers', NO_ARGS, []) { List list, Field f ->
            if(f.index == 0) {
                list << [];
            }

            def o = f.asObject();
            println(o);
            list[-1].add(o);
            return list; }).get();

        expect:
        all.size() == 2;
        all[0] == [1, true, 42, 420, 4200, 3.14f, 3.14159265d, new Money(100_00)];
        all[1] == [2, false, 43, 430, 4300, 2.71f, 2.71828182d, new Money(37_500_00)];
    }

    def "Test Short Stream"() {
        setup:
        List list = session(applyFields('select * from items', NO_ARGS, []) { List list, Field f ->
            if(f.index == 0) {
                list << [];
            }

            if(f.fieldDescriptor.name == 'description') {
                CharBuffer buf = CharBuffer.wrap(new char[1024]);
                list[-1] << buf;
                f.stream(buf);
            }
            else {
                list[-1] << f.asObject();
            }

            return list; }).get();

        expect:
        list.size() == 2;
        list[0][0] == 1;
        list[0][1] == 1;
        list[0][2].flip().toString()  == 'one';
    }
}
