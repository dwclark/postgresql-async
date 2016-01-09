package db.postgresql.async.tasks;

import db.postgresql.async.CommandStatus;
import db.postgresql.async.CompletableTask;
import db.postgresql.async.PostgresqlException;
import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.TransactionStatus;
import db.postgresql.async.TaskIterator;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.Response;
import db.postgresql.async.pginfo.StatementCache;
import db.postgresql.async.MultiStageException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MultiStageTask<T> implements CompletableTask<T> {

    private final TaskIterator<T> iter;
    private final CompletableFuture<T> future;
    private Task<?> current;
    private TaskState nextState;
    private Map<BackEnd,Consumer<Response>> oobHandlers;
    private StatementCache cache;
    private List<Task<?>> completed = new ArrayList<>();
        
    public MultiStageTask(final TaskIterator<T> iter) {
        this.iter = iter;
        this.future = new CompletableFuture<>();
        if(iter.hasNext(current)) {
            this.current = iter.next();
            this.nextState = current.getNextState();
        }
    }

    public CompletableFuture<T> getFuture() {
        return future;
    }

    private void processNextState() {
        current = iter.next();
        nextState = current.getNextState();
        current.setOobHandlers(oobHandlers);
        current.setStatementCache(cache);
    }
    
    private void process() {
        nextState = current.getNextState();

        if(nextState.next == TaskState.Next.FINISHED) {
            completed.add(current);
        }
        
        if(nextState.next == TaskState.Next.FINISHED && !iter.hasNext(current)) {
            List<Task<?>> failedTasks = completed.stream()
                .filter((t) -> t.getError() != null)
                .collect(Collectors.toList());
            
            if(failedTasks.size() == 0) {
                future.complete(getResult());
            }
            else if(failedTasks.size() == 1) {
                future.completeExceptionally(failedTasks.get(0).getError());
            }
            else {
                future.completeExceptionally(new MultiStageException(failedTasks));
            }
        }
        else if(nextState.next == TaskState.Next.FINISHED && iter.hasNext(current)) {
            processNextState();
        }
        else {
            nextState = current.getNextState();
        }
    }

    public T getResult() {
        return iter.getAccumulator();
    }
    
    public PostgresqlException getError() {
        return current.getError();
    }

    public CommandStatus getCommandStatus() {
        return current.getCommandStatus();
    }

    public TransactionStatus getTransactionStatus() {
        return current.getTransactionStatus();
    }

    public TaskState getNextState() {
        return nextState;
    }
    
    public void onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        current.onStart(fe, readBuffer);
        process();
    }
    
    public void onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        current.onRead(fe, readBuffer);
        process();
    }
    
    public void onWrite(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        current.onWrite(fe, readBuffer);
        process();
    }
    
    public void onFail(final Throwable t) {
        current.onFail(t);
        future.completeExceptionally(t);
    }
    
    public void onTimeout(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        current.onTimeout(fe, readBuffer);
        process();
    }

    public long getTimeout() {
        return current.getTimeout();
    }
    
    public TimeUnit getUnits() {
        return current.getUnits();
    }
    
    public void setOobHandlers(final Map<BackEnd,Consumer<Response>> oobHandlers) {
        this.oobHandlers = oobHandlers;
        current.setOobHandlers(oobHandlers);
    }

    public void setStatementCache(final StatementCache cache) {
        this.cache = cache;
        current.setStatementCache(cache);
    }
}
