package db.postgresql.async.tasks;

import db.postgresql.async.CommandStatus;
import db.postgresql.async.Field;
import db.postgresql.async.PostgresqlException;
import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.TransactionStatus;
import db.postgresql.async.messages.*;
import db.postgresql.async.pginfo.StatementCache;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.BiFunction;

public abstract class BaseTask<T> implements Task<T> {

    private final Map<BackEnd,Consumer<Response>> oobHandlers = new EnumMap<>(BackEnd.class);
    private final long timeout;
    private final TimeUnit units;
    private final RowMode rowMode;

    private boolean executed;
    private Throwable error;
    protected TaskState nextState = TaskState.start();
    protected CommandComplete commandComplete;
    protected ReadyForQuery readyForQuery;
    protected Response current;
    protected T accumulator;
    protected BiFunction<T,Field,T> fieldFunction;
    
    public void executed() {
        executed = true;
    }

    public boolean isExecuted() {
        return executed;
    }

    public boolean getHasErrorResponse() {
        return ((error != null) && (error instanceof PostgresqlException));
    }
    
    public CommandStatus getCommandStatus() {
        return commandComplete;
    }

    public TransactionStatus getTransactionStatus() {
        if(readyForQuery == null) {
            return null;
        }
        else {
            return readyForQuery.getStatus();
        }
    }

    public TaskState getNextState() {
        return nextState;
    }
    
    public BaseTask() {
        this(0L, TimeUnit.SECONDS);
    }

    public BaseTask(final long timeout, final TimeUnit units) {
        this(timeout, units, RowMode.ROW, null, null);
    }

    public BaseTask(final long timeout, final TimeUnit units, final RowMode rowMode,
                    final T accumulator, final BiFunction<T,Field,T> fieldFunction) {
        this.timeout = timeout;
        this.units = units;
        this.rowMode = rowMode;
        this.accumulator = accumulator;
        this.fieldFunction = fieldFunction;
    }

    public Throwable getError() {
        return error;
    }

    public void setError(final Throwable val) {
        error = val;
    }

    public abstract void onStart(FrontEndMessage fe, ByteBuffer readBuffer);
    public abstract void onRead(FrontEndMessage fe, ByteBuffer readBuffer);
    
    public void onWrite(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        nextState = TaskState.read();
    }
    
    public int pump(final ByteBuffer readBuffer, final Predicate<Response> processor) {
        int needs = 0;
        boolean keepGoing = true;
        while(keepGoing &&
              readBuffer.hasRemaining() &&
              needs == 0) {
            if(rowMode == RowMode.ROW) {
                current = Response.rowMode(readBuffer);
            }
            else if(current == null && rowMode == RowMode.FIELD) {
                current = Response.fieldMode(readBuffer, accumulator, fieldFunction);
            }

            current.networkComplete(readBuffer);

            if(current.getNeeds() > 0) {
                needs = current.getNeeds();
                if(current.isFinished()) {
                    current = null;
                }
                
                continue;
            }
            
            if(current.getBackEnd().outOfBand) {
                onOob(current);
                keepGoing = true;
            }
            else if(current.getBackEnd() == BackEnd.ErrorResponse) {
                onError((Notice) current);
                keepGoing = true;
            }
            else if(!(current instanceof StreamingRow)) {
                keepGoing = processor.test(current);
            }

            if(current.isFinished()) {
                current = null;
            }
        }

        return needs;
    }
    
    public void onFail(Throwable t) {
        nextState = TaskState.terminate();
    }

    public void onTimeout(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        nextState = TaskState.finished();
    }

    public void onError(final Notice val) {
        PostgresqlException e = val.toException();
        setError(e);
        nextState = TaskState.finished();
    }

    public void onOob(final Response r) {
        if(!r.getBackEnd().outOfBand) {
            throw new IllegalArgumentException(r.getBackEnd() + " is not an out of band type");
        }

        oobHandlers.get(r.getBackEnd()).accept(r);
    }

    public void addOobHandler(final BackEnd backEnd, final Consumer<Response> handler) {
        oobHandlers.put(backEnd, handler);
    }

    public void setOobHandlers(final Map<BackEnd,Consumer<Response>> oobHandlers) {
        this.oobHandlers.putAll(oobHandlers);
    }

    public void setStatementCache(final StatementCache cache) {
        //not needed
    }

    public void clearOobHandlers() {
        oobHandlers.clear();
    }
    
    public long getTimeout() { return timeout; }
    public TimeUnit getUnits() { return units; }
}
