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
import db.postgresql.async.serializers.SerializationContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ExecuteTask<T> extends BaseTask<T> {

    private final String sql;
    private final List<List<Object>> args;
    private final BiFunction<T,Row,T> func;
    protected T accumulator;

    private ReadyForQuery readyForQuery;
    private PgSessionCache cache;
    private int executionCount = 0;
    private TaskPhase phase;
    
    public ExecuteTask(final String sql, final List<List<Object>> args,
                       final T accumulator,
                       final BiFunction<T,Row,T> func) {
        this.sql = sql;
        this.args = args;
        this.accumulator = accumulator;
        this.func = func;
        this.phase = new PreparePhase();
    }

    private interface TaskPhase {
        void onRead(FrontEndMessage feMessage, ByteBuffer readBuffer);
        void onWrite(FrontEndMessage feMessage, ByteBuffer readBuffer);
        void onStart(FrontEndMessage feMessage, ByteBuffer readBuffer);
    }

    public class PreparePhase implements TaskPhase {
        private String id;
        private RowDescription rowDescription;
        private ParameterDescription parameterDescription;
        private int rfqCount = 0;
        
        public boolean isNeeded() {
            return cache.statement(sql) == null;
        }

        private void computeNextState(final int needs, final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
            if(needs > 0) {
                nextState = TaskState.needs(needs);
            }
            else if(rfqCount == 2) {
                phase = new ExecutePhase();
                phase.onStart(feMessage, readBuffer);
            }
            else {
                nextState = TaskState.read();
            }
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

        public void onRead(final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
            computeNextState(pump(readBuffer, this::readProcessor), feMessage, readBuffer);
        }

        public void onWrite(FrontEndMessage feMessage, ByteBuffer readBuffer) {
            nextState = TaskState.read();
        }
        
        public void onStart(final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
            if(isNeeded()) {
                id = Statement.nextId();
                feMessage.parse(id, sql, FrontEndMessage.EMPTY_OIDS);
                feMessage.sync();
                feMessage.describeStatement(id);
                feMessage.sync();
                nextState = TaskState.write();
            }
            else {
                phase = new ExecutePhase();
                phase.onStart(feMessage, readBuffer);
            }
        }
    }

    public class ExecutePhase implements TaskPhase {

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

        private void computeNextState(final int needs) {
            if(needs > 0) {
                nextState = TaskState.needs(needs);
            }
            else if(readyForQuery != null) {
                nextState = TaskState.finished();
            }
            else {
                nextState = TaskState.read();
            }
        }

        private void writePossible(final FrontEndMessage feMessage, final ByteBuffer buffer) {
            final Statement statement = cache.statement(sql);
            boolean wrote = false;
            while(executionCount < args.size()) {
                final boolean success = feMessage.bindExecuteSync(statement, args.get(executionCount));
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
                SerializationContext.description(statement.getRowDescription());
                nextState = TaskState.read();
            }
        }

        public void onRead(final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
            computeNextState(pump(readBuffer, this::readProcessor));
        }

        public void onWrite(final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
            writePossible(feMessage, readBuffer);
        }
        
        public void onStart(FrontEndMessage feMessage, ByteBuffer readBuffer) {
            writePossible(feMessage, readBuffer);
        }
    }

    @Override
    public void setStatementCache(final PgSessionCache cache) {
        this.cache = cache;
    }

    protected void onDataRow(final DataRow dataRow) {
        accumulator = func.apply(accumulator, dataRow);
    }

    protected void onCommandComplete(final CommandComplete val) {
        commandComplete = val;
        --executionCount;
    }

    public T getResult() {
        return accumulator;
    }

    public void onRead(final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
        phase.onRead(feMessage, readBuffer);
    }
    
    public void onWrite(final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
        phase.onWrite(feMessage, readBuffer);
    }
    
    public void onStart(final FrontEndMessage feMessage, final ByteBuffer readBuffer) {
        phase.onStart(feMessage, readBuffer);
    }

    public static class BulkExecute extends ExecuteTask<List<Integer>> {
        
        public BulkExecute(final String sql, final List<List<Object>> args) {
            super(sql, args, new ArrayList<>(), null);
        }

        @Override
        public void onCommandComplete(final CommandComplete val) {
            super.onCommandComplete(val);
            accumulator.add(val.getRows());
        }
    }

    public static class Execute extends ExecuteTask<Integer> {
        public Execute(final String sql, final List<Object> args) {
            super(sql, Collections.singletonList(args), 0, null);
        }

        @Override
        public void onCommandComplete(final CommandComplete val) {
            super.onCommandComplete(val);
            accumulator = val.getRows();
        }
    }
}
