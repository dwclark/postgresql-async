package db.postgresql.async.tasks;

import db.postgresql.async.Concurrency;
import db.postgresql.async.QueryPart;
import db.postgresql.async.Isolation;
import db.postgresql.async.RwMode;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.BiFunction;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.List;

public abstract class SimpleTask<T> extends BaseTask<T> {

    private final String sql;

    public String getSql() {
        return sql;
    }
    
    protected T accumulator;
    
    public SimpleTask(final String sql, final T accumulator) {
        this.sql = sql;
        this.accumulator = accumulator;
    }
    
    protected boolean readProcessor(final Response resp) {
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
        case NoData:
            SerializationContext.description(RowDescription.EMPTY);
            return true;
        default:
            setError(new UnsupportedOperationException(resp.getBackEnd() + " is not supported by simple task"));
            return false;
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
        fe.query(getSql());
        nextState = TaskState.write();
    }

    public static class NoOutput extends SimpleTask<Void> {

        private final boolean terminal;
        
        public NoOutput(final String sql, final boolean terminal) {
            super(sql, null);
            this.terminal = terminal;
        }

        @Override
        public boolean isTerminal() {
            return terminal;
        }

        public Void getResult() {
            return null;
        }

        public void onDataRow(final DataRow dataRow) {
            setError(new UnsupportedOperationException());
        }
    }

    public static class Execute extends SimpleTask<Integer> {
        private final Consumer<Integer> consumer;
        
        public Execute(final String sql) {
            this(sql, (i) -> {});
        }

        public Execute(final String sql, final Consumer<Integer> consumer) {
            super(sql, null);
            this.consumer = consumer;
        }

        public Integer getResult() {
            return commandComplete.getRows();
        }

        @Override
        public void onCommandComplete(final CommandComplete val) {
            super.onCommandComplete(val);
            accumulator = val.getRows();
            consumer.accept(val.getRows());
        }

        public void onDataRow(final DataRow dataRow) {
            setError(new UnsupportedOperationException());
        }
    }

    public static class Query<T> extends SimpleTask<T> {
        private final BiFunction<T,Row,T> func;
            
        public Query(final String sql, final T accumulator,
                     final BiFunction<T, Row, T> func) {
            super(sql, accumulator);
            this.func = func;
        }

        public void onDataRow(final DataRow dataRow) {
            try {
                dataRow.with(() -> this.accumulator = func.apply(accumulator, dataRow));
            }
            catch(Throwable t) {
                setError(t);
            }
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
            try {
                current.onDataRow(dataRow);
            }
            catch(Throwable t) {
                setError(t);
            }
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
                if(fe.query(current.getSql())) {
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

    public static <T> SimpleTask<T> query(final String sql, final T accumulator,
                                          final BiFunction<T,Row,T> func) {
        return new Query<>(sql, accumulator, func);
    }

    public static <T> SimpleTask<List<T>> query(final String sql, final Function<Row,T> func) {
        final BiFunction<List<T>, Row, List<T>> biFunc = (list, row) -> {
            list.add(func.apply(row));
            return list;
        };

        return query(sql, new ArrayList<>(), biFunc);
    }

    public static SimpleTask<Integer> execute(final String sql) {
        return new Execute(sql);
    }

    public static SimpleTask<List<Object>> multi(final List<QueryPart<?>> parts) {
        return new Multi(parts);
    }

    public static SimpleTask<Void> noOutput(final String sql) {
        return new NoOutput(sql, false);
    }

    public static SimpleTask<Void> begin(final Concurrency concurrency) {
        return begin(concurrency.getIsolation(), concurrency.getMode(), concurrency.getDeferrable());
    }

    public static SimpleTask<Void> begin(final Isolation isolation, final RwMode mode, final boolean deferrable) {
        final String sql = String.format("BEGIN ISOLATION LEVEL %s %s %s;", isolation, mode,
                                         deferrable ? "DEFERRABLE" : "NOT DEFERRABLE");
        return noOutput(sql);
    }

    public static SimpleTask<Void> commit() {
        return new NoOutput("commit;", true);
    }

    public static SimpleTask<Void> rollback() {
        return new NoOutput("rollback;", true);
    }
}
