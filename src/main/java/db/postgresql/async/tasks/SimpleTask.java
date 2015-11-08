package db.postgresql.async.tasks;

import db.postgresql.async.CommandStatus;
import db.postgresql.async.Row;
import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.CommandComplete;
import db.postgresql.async.messages.DataRow;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.Response;
import db.postgresql.async.messages.RowDescription;
import db.postgresql.async.serializers.SerializationContext;
import java.nio.ByteBuffer;
import java.util.function.BiFunction;

public abstract class SimpleTask<T> extends BaseTask<T> {

    private final String sql;
    protected T accumulator;
    private CommandComplete commandComplete;
    
    public SimpleTask(final String sql, final T accumulator) {
        this.sql = sql;
        this.accumulator = accumulator;
    }
    
    public CommandStatus getCommandStatus() { return commandComplete; }
    public T getResult() { return accumulator; }

    private boolean readProcessor(final Response resp) {
        switch(resp.getBackEnd()) {
        case RowDescription:
            SerializationContext.description((RowDescription) resp);
            return true;
        case DataRow:
            onDataRow((DataRow) resp);
            return true;
        case CommandComplete:
            commandComplete = (CommandComplete) resp;
            return false;
        default:
            throw new UnsupportedOperationException(resp.getBackEnd() + " is not supported by simple task");
        }
    }

    abstract public void onDataRow(DataRow dataRow);
    
    public TaskState onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        return pump(readBuffer, this::readProcessor, () -> TaskState.finished());
    }

    public TaskState onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        fe.query(sql);
        return TaskState.write();
    }

    private static class ForExecute extends SimpleTask<Void> {
        public ForExecute(final String sql) {
            super(sql, null);
        }

        public void onDataRow(final DataRow dataRow) {
            throw new UnsupportedOperationException();
        }
    }

    public static SimpleTask<Void> forExecute(final String sql) {
        return new ForExecute(sql);
    }

    private static class ForQuery<T> extends SimpleTask<T> {
        private final BiFunction<T,Row,T> func;
            
        public ForQuery(final String sql, final T accumulator,
                        final BiFunction<T,Row,T> func) {
            super(sql, accumulator);
            this.func = func;
        }

        public void onDataRow(final DataRow dataRow) {
            try {
                this.accumulator = func.apply(accumulator, dataRow);
            }
            finally {
                dataRow.finish();
            }
        }
    }

    public static <T> SimpleTask<T> forQuery(final String sql, final T accumulator,
                                             final BiFunction<T,Row,T> func) {
        return new ForQuery<>(sql, accumulator, func);
    }
}
