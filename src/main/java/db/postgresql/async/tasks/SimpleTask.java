package db.postgresql.async.tasks;

import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.DataRowIterator;
import db.postgresql.async.messages.Response;
import db.postgresql.async.messages.RowDescription;
import db.postgresql.async.messages.DataRow;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.BackEnd;
import java.util.function.BiFunction;
import java.nio.ByteBuffer;

public class SimpleTask extends BaseTask {

    private RowDescription description;
    private final String sql;
    
    public SimpleTask(final String sql) {
        this.sql = sql;
    }

    private boolean readProcessor(final Response resp) {
        switch(resp.getBackEnd()) {
        case RowDescription:
            description = (RowDescription) resp;
            return true;
        case DataRow:
            //final DataRow dr = (DataRow) resp;
            //accumulator = function.apply(accumulator, dr.iterator
            return true;
        default:
            throw new UnsupportedOperationException(resp.getBackEnd() + " is not supported by simple task");
        }
    }
    
    public TaskState onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        return null;
    }

    public TaskState onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        fe.query(sql);
        return TaskState.write();
    }
}
