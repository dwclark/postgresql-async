package db.postgresql.async.tasks;

import db.postgresql.async.Action;
import db.postgresql.async.CommandStatus;
import db.postgresql.async.Result;
import db.postgresql.async.Row;
import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.TransactionStatus;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.CommandComplete;
import db.postgresql.async.messages.DataRow;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.Response;
import db.postgresql.async.messages.ReadyForQuery;
import db.postgresql.async.messages.RowDescription;
import db.postgresql.async.serializers.SerializationContext;
import java.nio.ByteBuffer;
import java.util.function.BiFunction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class SimpleTask<T> extends BaseTask<T> {

    private final String sql;
    protected T accumulator;
    protected CommandComplete commandComplete;
    protected ReadyForQuery readyForQuery;
    
    public SimpleTask(final String sql, final T accumulator) {
        this.sql = sql;
        this.accumulator = accumulator;
    }
    
    public TransactionStatus getTransactionStatus() {
        return readyForQuery.getStatus();
    }

    public CommandStatus getCommandStatus() {
        return commandComplete;
    }

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
            return true;
        case ReadyForQuery:
            readyForQuery = (ReadyForQuery) resp;
            return false;
        default:
            throw new UnsupportedOperationException(resp.getBackEnd() + " is not supported by simple task");
        }
    }

    abstract public void onDataRow(DataRow dataRow);

    protected void onCommandComplete(final CommandComplete commandComplete) {
        this.commandComplete = commandComplete;
    }
    
    public TaskState onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        return pump(readBuffer, this::readProcessor, () -> TaskState.finished());
    }

    public TaskState onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        fe.query(sql);
        return TaskState.write();
    }

    private static class ForExecute extends SimpleTask<Integer> {
        public ForExecute(final String sql) {
            super(sql, null);
        }

        public Integer getResult() {
            return commandComplete.getRows();
        }

        public void onDataRow(final DataRow dataRow) {
            throw new UnsupportedOperationException();
        }
    }

    public static SimpleTask<Integer> forExecute(final String sql) {
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
            Row.withRow(dataRow, () -> { this.accumulator = func.apply(accumulator, dataRow); });
        }

        public T getResult() {
            return accumulator;
        }
    }

    public static <T> SimpleTask<T> forQuery(final String sql, final T accumulator,
                                             final BiFunction<T,Row,T> func) {
        return new ForQuery<>(sql, accumulator, func);
    }

    public static class Multi extends SimpleTask<List<Object>> {
        private final Iterator<QueryPart<?>> iter;
        private QueryPart<?> current;
        
        public Multi(final String sql, final List<QueryPart<?>> parts) {
            super(sql, new ArrayList<>(parts.size()));
            this.iter = parts.iterator();
            current = iter.next();
        }
        
        public void onDataRow(final DataRow dataRow) {
            current.onDataRow(dataRow);
        }
        
        protected void onCommandComplete(final CommandComplete commandComplete) {
            this.commandComplete = commandComplete;
            if(commandComplete.getAction() == Action.SELECT) {
                accumulator.add(current.accumulator);
            }
            else {
                accumulator.add(commandComplete.getRows());
            }

            if(iter.hasNext()) {
                current = iter.next();
            }
        }

        public List<Object> getResult() {
            return accumulator;
        }
    }

    public static SimpleTask<List<Object>> forMulti(final List<QueryPart<?>> parts) {
        StringBuilder sb = new StringBuilder();
        for(QueryPart<?> part : parts) {
            String sql = part.sql.trim();
            sb.append(sql);
            if(!sql.endsWith(";")) {
                sb.append(";");
            }
        }

        return new Multi(sb.toString(), parts);
    }
}
