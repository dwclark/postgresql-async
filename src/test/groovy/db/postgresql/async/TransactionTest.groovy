package db.postgresql.async;

import spock.lang.*;
import db.postgresql.async.serializers.*;
import db.postgresql.async.tasks.*;
import db.postgresql.async.types.*;
import db.postgresql.async.*;
import db.postgresql.async.enums.*;
import java.time.*;
import static db.postgresql.async.Task.*;

class TransactionTest extends Specification {

    @Shared Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }

    def cleanupSpec() {
        session.shutdown();
    }

    def "Minimal Transaction"() {
        setup:
        def accum = [];
        CompletableTask t = transaction().accumulator(accum).stage { a ->
            return prepared('select * from all_types;', [], accum) { list, row ->
                list << row.toList(); }; }.build();
        session.execute(t).get();
        
        expect:
        accum.size() == 1;
        println(accum[0]);
    }

}
