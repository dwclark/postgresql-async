package db.postgresql.async;

import spock.lang.*;
import org.junit.rules.TemporaryFolder;
import org.junit.Rule;
import static db.postgresql.async.Task.Copy.*;

class CopyTest extends Specification {

    private static Session session;
    
    def setupSpec() {
        session = Helper.noAuthLoadTypes();
    }
    
    def cleanupSpec() {
        session.shutdown();
    }

    @Rule public TemporaryFolder folder;

    def "Copy From Server Basic"() {
        setup:
        File file = folder.newFile("numerals.bin");
        long total = session(fromServer('copy numerals from stdin with format binary', file)).get();

        expect:
        total > 0;
    }
}
