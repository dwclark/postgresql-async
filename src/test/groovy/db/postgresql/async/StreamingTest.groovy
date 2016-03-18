package db.postgresql.async;

import java.util.function.Consumer;
import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.types.*;
import db.postgresql.async.*;
import db.postgresql.async.enums.*;
import java.time.*;
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

            List current = list[-1];
            current << f.asObject(); }).get();

        expect:
        all.size() == 2;
        all[0].size() == 8;
    }
}
