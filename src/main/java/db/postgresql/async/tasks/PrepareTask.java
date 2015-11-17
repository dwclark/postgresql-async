package db.postgresql.async.tasks;

import db.postgresql.async.TaskState;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.ParameterDescription;
import db.postgresql.async.messages.Response;
import db.postgresql.async.messages.RowDescription;
import db.postgresql.async.pginfo.PgSessionCache;
import db.postgresql.async.pginfo.Statement;
import java.nio.ByteBuffer;

public class PrepareTask extends BaseTask<Void> {

    private final String sql;
    private final String id;
    private final PgSessionCache cache;
    private RowDescription rowDescription;
    private ParameterDescription parameterDescription;
    private int rfqCount = 0;

    public String getId() { return id; }
    
    public PrepareTask(final String sql, final PgSessionCache cache) {
        this.sql = sql;
        this.cache = cache;
        this.id = Statement.nextId();
    }

    private boolean readProcessor(final Response resp) {
        switch(resp.getBackEnd()) {
        case RowDescription:
            rowDescription = (RowDescription) resp;
            return true;
        case ParameterDescription:
            parameterDescription = (ParameterDescription) resp;
            return true;
        case ParseComplete:
            return true;
        case ReadyForQuery:
            ++rfqCount;
            if(rfqCount == 1) {
                return true;
            }
            else {
                final Statement stmt = new Statement(id, parameterDescription, rowDescription);
                cache.store(sql, stmt);
                return false;
            }
        default:
            throw new UnsupportedOperationException(resp.getBackEnd() + " is not a valid response");
        }
    }

    private void computeNextState(final int needs) {
        if(needs > 0) {
            nextState = TaskState.needs(needs);
        }
        else if(rfqCount == 2) {
            nextState = TaskState.finished();
        }
        else {
            nextState = TaskState.read();
        }
    }
    
    public void onRead(final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
        computeNextState(pump(readBuffer, this::readProcessor));
    }

    public void onStart(final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
        feMessage.parse(id, sql, FrontEndMessage.EMPTY_OIDS);
        feMessage.sync();
        feMessage.describeStatement(id);
        feMessage.sync();
        nextState = TaskState.write();
    }

    public Void getResult() {
        return null;
    }
}
