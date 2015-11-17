package db.postgresql.async.tasks;

import db.postgresql.async.Row;
import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.CommandComplete;
import db.postgresql.async.messages.DataRow;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.ParameterDescription;
import db.postgresql.async.messages.ReadyForQuery;
import db.postgresql.async.messages.Response;
import db.postgresql.async.messages.RowDescription;
import db.postgresql.async.pginfo.PgSessionCache;
import db.postgresql.async.pginfo.Statement;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ExecuteTask<T> extends BaseTask<T> {

    private final String sql;
    private final List<Object[]> args;
    private final BiFunction<T,Row,T> func;
    protected T accumulator;

    private PgSessionCache cache;
    private int executionCount = 0;
    
    public ExecuteTask(final String sql, final List<Object[]> args,
                       final T accumulator,
                       final BiFunction<T,Row,T> func) {
        this.sql = sql;
        this.args = args;
        this.accumulator = accumulator;
        this.func = func;
    }

    public RowDescription getRowDescription() {
        return cache.statement(sql).getRowDescription();
    }

    @Override
    public Task<Void> prepareTask(final PgSessionCache cache) {
        this.cache = cache;
        if(cache.statement(sql) == null) {
            return new PrepareTask(sql, cache);
        }
        else {
            return null;
        }
    }

    private boolean readProcessor(final Response resp) {
        switch(resp.getBackEnd()) {
        case DataRow:
            onDataRow((DataRow) resp);
            return true;
        case BindComplete:
            return true;
        case CommandComplete:
            onCommandComplete((CommandComplete) resp);
            return true;
        case ReadyForQuery:
            readyForQuery = (ReadyForQuery) resp;
            return false;
        default:
            throw new UnsupportedOperationException(resp.getBackEnd() + " is not a valid response");
        }
    }

    protected void onDataRow(final DataRow dataRow) {
        accumulator = func.apply(accumulator, dataRow);
    }

    protected void onCommandComplete(final CommandComplete val) {
        commandComplete = val;
        --executionCount;
    }

    private void computeNextState(final int needs) {
        if(needs > 0) {
            nextState = TaskState.needs(needs);
        }
        else if(readyForQuery == null) {
            nextState = TaskState.finished();
        }
        else {
            nextState = TaskState.read();
        }
    }
    
    public void onRead(final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
        computeNextState(pump(readBuffer, this::readProcessor));
    }

    private void writePossible(final FrontEndMessage feMessage, final ByteBuffer buffer) {
        final Statement statement = cache.statement(sql);
        boolean wrote = false;
        while(executionCount < args.size()) {
            final boolean success = feMessage.bindAndExecute(statement, args.get(executionCount));
            if(success) {
                ++executionCount;
                wrote = true;
            }
            else {
                break;
            }
        }

        if(wrote) {
            nextState = TaskState.write();
        }
        else {
            nextState = TaskState.read();
        }
    }

    public void onWrite(final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
        writePossible(feMessage, readBuffer);
    }

    public void onStart(final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
        writePossible(feMessage, readBuffer);
    }

    public T getResult() {
        return accumulator;
    }

    private static class BulkExecute extends ExecuteTask<List<Integer>> {
        
        public BulkExecute(final String sql, final List<Object[]> args) {
            super(sql, args, new ArrayList<>(), null);
        }

        @Override
        public void onCommandComplete(final CommandComplete val) {
            super.onCommandComplete(val);
            accumulator.add(val.getRows());
        }
    }

    private static class Execute extends ExecuteTask<Integer> {
        public Execute(final String sql, final Object[] args) {
            super(sql, Collections.singletonList(args), 0, null);
        }

        @Override
        public void onCommandComplete(final CommandComplete val) {
            super.onCommandComplete(val);
            accumulator = val.getRows();
        }
    }

    public static ExecuteTask<List<Integer>> forBulkExecute(final String sql, final List<Object[]> args) {
        return new BulkExecute(sql, args);
    }

    public static ExecuteTask<Integer> forExecute(final String sql, final Object[] args) {
        return new Execute(sql, args);
    }

    public static <R> ExecuteTask<List<R>> forQuery(final String sql, final Object[] args, final Function<Row,R> func) {
        final BiFunction<List<R>,Row,List<R>> biFunc = (list,row) -> { list.add(func.apply(row)); return list; };
        return new ExecuteTask<>(sql, Collections.singletonList(args),
                                 new ArrayList<>(), biFunc);
    }
}