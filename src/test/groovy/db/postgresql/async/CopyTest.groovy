package db.postgresql.async;

import spock.lang.*;
import org.junit.rules.TemporaryFolder;
import org.junit.Rule;
import static db.postgresql.async.Task.Copy.*;
import static db.postgresql.async.Task.Simple.*;

class CopyTest extends Specification {

    private static Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }
    
    def cleanupSpec() {
        session.shutdown();
    }

    @Rule public TemporaryFolder folder;

    def "Copy To/From Server Basic"() {
        when:
        File file = folder.newFile("numerals.csv");
        long total = session(fromServer("copy numerals to stdin with (format 'text', delimiter ',');", file)).get();

        then:
        total > 0;

        when:
        session(noOutput('truncate table numerals;')).get();
        int size = session(applyRows('select count(*) from numerals;', { r -> r.iterator().nextInt(); })).get()[0];
        
        then:
        size == 0;

        when:
        long total2 = session(toServer("copy numerals from stdin with (format'text', delimiter ',');", file)).get();
        int size2 = session(applyRows('select count(*) from numerals;', { r -> r.iterator().nextInt(); })).get()[0];
        
        then:
        total2 > 0;
        size2 == 20;
    }
}
