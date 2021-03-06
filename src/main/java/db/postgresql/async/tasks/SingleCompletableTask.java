package db.postgresql.async.tasks;

import db.postgresql.async.CommandStatus;
import db.postgresql.async.CompletableTask;
import db.postgresql.async.PostgresqlException;
import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.TransactionStatus;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.Response;
import db.postgresql.async.pginfo.StatementCache;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class SingleCompletableTask<T> implements CompletableTask<T> {

    private final Task<T> task;
    private final CompletableFuture<T> future;
    
    public SingleCompletableTask(final Task<T> task) {
        this.task = task;
        this.future = new CompletableFuture<>();
    }

    public void executed() {
        task.executed();
    }

    public boolean isExecuted() {
        return task.isExecuted();
    }
    
    public CompletableFuture<T> getFuture() {
        return future;
    }
    
    private void process() {
        final TaskState state = getNextState();
        if(state.next== TaskState.Next.FINISHED || state.next == TaskState.Next.TERMINATE) {
            if(task.getError() == null) {
                future.complete(task.getResult());
            }
            else {
                future.completeExceptionally(task.getError());
            }
        }
    }

    @Override
    public T getResult() {
        return task.getResult();
    }

    @Override
    public Throwable getError() {
        return task.getError();
    }

    @Override
    public void setError(final Throwable t) {
        task.setError(t);
    }

    @Override
    public CommandStatus getCommandStatus() {
        return task.getCommandStatus();
    }

    @Override
    public TransactionStatus getTransactionStatus() {
        return task.getTransactionStatus();
    }

    @Override
    public TaskState getNextState() {
        return task.getNextState();
    }

    @Override
    public void onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        task.onStart(fe, readBuffer);
        process();
    }

    @Override
    public void onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        task.onRead(fe, readBuffer);
        process();
    }

    @Override
    public void onWrite(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        task.onWrite(fe, readBuffer);
        process();
    }

    @Override
    public void onFail(final Throwable t) {
        task.onFail(t);
        future.completeExceptionally(t);
    }

    @Override
    public void onTimeout(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        task.onTimeout(fe, readBuffer);
        process();
    }

    @Override
    public long getTimeout() {
        return task.getTimeout();
    }

    @Override
    public TimeUnit getUnits() {
        return task.getUnits();
    }

    @Override
    public void setOobHandlers(final Map<BackEnd,Consumer<Response>> oobHandlers) {
        task.setOobHandlers(oobHandlers);
    }

    @Override
    public void setStatementCache(final StatementCache cache) {
        task.setStatementCache(cache);
    }
}
