package db.postgresql.async.tasks;

import db.postgresql.async.NullOutput;
import db.postgresql.async.Isolation;
import db.postgresql.async.RwMode;
import db.postgresql.async.CommandStatus;
import db.postgresql.async.Row;
import db.postgresql.async.TaskState;
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
import java.util.ListIterator;
import java.util.List;

public abstract class SimpleTask<T> extends BaseTask<T> {

    private final String sql;
    protected T accumulator;
    
    public SimpleTask(final String sql, final T accumulator) {
        this.sql = sql;
        this.accumulator = accumulator;
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
            onCommandComplete((CommandComplete) resp);
            return true;
        case ReadyForQuery:
            return onReadyForQuery((ReadyForQuery) resp);
        default:
            throw new UnsupportedOperationException(resp.getBackEnd() + " is not supported by simple task");
        }
    }

    abstract public void onDataRow(DataRow dataRow);

    protected void onCommandComplete(final CommandComplete commandComplete) {
        this.commandComplete = commandComplete;
    }

    protected boolean onReadyForQuery(final ReadyForQuery val) {
        readyForQuery = val;
        return false;
    }

    public void computeNextState(final int needs) {
        if(needs > 0) {
            nextState = TaskState.needs(needs);
        }
        else if(readyForQuery == null) {
            nextState = TaskState.read();
        }
        else {
            nextState = TaskState.finished();
        }
    }

    public void onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        computeNextState(pump(readBuffer, this::readProcessor));
    }

    public void onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        fe.query(sql);
        nextState = TaskState.write();
    }

    private static class NoOutput extends SimpleTask<NullOutput> {
        public NoOutput(final String sql) {
            super(sql, null);
        }

        public NullOutput getResult() {
            return NullOutput.instance;
        }

        public void onDataRow(final DataRow dataRow) {
            throw new UnsupportedOperationException();
        }
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

    private static class ForQuery<T> extends SimpleTask<T> {
        private final BiFunction<T,Row,T> func;
            
        public ForQuery(final String sql, final T accumulator,
                        final BiFunction<T,Row,T> func) {
            super(sql, accumulator);
            this.func = func;
        }

        public void onDataRow(final DataRow dataRow) {
            dataRow.with(() -> this.accumulator = func.apply(accumulator, dataRow));
        }

        public T getResult() {
            return accumulator;
        }
    }

    public static class Multi extends SimpleTask<List<Object>> {
        private final List<QueryPart<?>> parts;
        private ListIterator<QueryPart<?>> iter;
        private QueryPart<?> current;
        
        public Multi(final List<QueryPart<?>> parts) {
            super(null, new ArrayList<>(parts.size()));
            this.parts = parts;
        }

        @Override
        public void onDataRow(final DataRow dataRow) {
            current.onDataRow(dataRow);
        }

        @Override
        protected void onCommandComplete(final CommandComplete commandComplete) {
            this.commandComplete = commandComplete;
            if(commandComplete.isSelect()) {
                accumulator.add(current.getAccumulator());
            }
            else if(commandComplete.isMutation()) {
                accumulator.add(commandComplete.getRows());
            }

            //ignore any non select, insert, update, delete commands
        }

        @Override
        protected boolean onReadyForQuery(final ReadyForQuery val) {
            if(iter.hasNext()) {
                this.current = iter.next();
                return true;
            }
            else {
                readyForQuery = val;
                return false;
            }
        }

        @Override
        public List<Object> getResult() {
            return accumulator;
        }

        @Override
        public void onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
            this.iter = parts.listIterator();
            writePossible(fe, readBuffer);
        }
        
        private void writePossible(final FrontEndMessage fe, final ByteBuffer readBuffer) {
            boolean wrote = false;
            while(iter.hasNext()) {
                this.current = iter.next();
                if(fe.query(current.sql)) {
                    wrote = true;
                }
                else {
                    this.current = iter.previous();
                }
            }

            if(wrote) {
                nextState = TaskState.write();
            }
            else {
                this.iter = parts.listIterator();
                this.current = iter.next();
                nextState = TaskState.read();
            }
        }

        @Override
        public void onWrite(final FrontEndMessage fe, final ByteBuffer readBuffer) {
            writePossible(fe, readBuffer);
        }
    }

    public static <T> SimpleTask<T> forQuery(final String sql, final T accumulator,
                                             final BiFunction<T,Row,T> func) {
        return new ForQuery<>(sql, accumulator, func);
    }

    public static SimpleTask<Integer> forExecute(final String sql) {
        return new ForExecute(sql);
    }

    public static SimpleTask<List<Object>> forMulti(final List<QueryPart<?>> parts) {
        return new Multi(parts);
    }

    public static SimpleTask<NullOutput> noOutput(final String sql) {
        return new NoOutput(sql);
    }

    public static SimpleTask<NullOutput> begin(final Isolation isolation, final RwMode mode, final boolean deferrable) {
        final String sql = String.format("BEGIN ISOLATION LEVEL %s %s %s;", isolation, mode,
                                         deferrable ? "DEFERRABLE" : "NOT DEFERRABLE");
        return noOutput(sql);
    }

    public static SimpleTask<NullOutput> commit() {
        return noOutput("commit;");
    }

    public static SimpleTask<NullOutput> rollback() {
        return noOutput("rollback;");
    }
}
