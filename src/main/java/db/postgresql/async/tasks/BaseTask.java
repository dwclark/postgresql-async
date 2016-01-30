package db.postgresql.async.tasks;

import db.postgresql.async.CommandStatus;
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

public abstract class BaseTask<T> implements Task<T> {

    private final Map<BackEnd,Consumer<Response>> oobHandlers = new EnumMap<>(BackEnd.class);
    private final long timeout;
    private final TimeUnit units;

    private boolean executed;
    private Throwable error;
    protected TaskState nextState = TaskState.start();
    protected CommandComplete commandComplete;
    protected ReadyForQuery readyForQuery;

    public void executed() {
        executed = true;
    }

    public boolean isExecuted() {
        return executed;
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
        this.timeout = timeout;
        this.units = units;
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
              (needs = BackEnd.needs(readBuffer)) == 0) {
            final int pos = readBuffer.position();
            final Response resp = BackEnd.find(readBuffer.get(pos)).builder.apply(readBuffer);

            if(resp.getBackEnd().outOfBand) {
                onOob(resp);
            }
            else if(resp.getBackEnd() == BackEnd.ErrorResponse) {
                onError((Notice) resp);
            }
            else {
                keepGoing = processor.test(resp);
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
