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

    public CompletableFuture<T> getFuture() {
        return future;
    }
    
    private TaskState process(final TaskState state) {
        if(state.next == TaskState.Next.FINISHED || state.next == TaskState.Next.TERMINATE) {
            if(task.getError() == null) {
                future.complete(task.getResult());
            }
            else {
                future.completeExceptionally(task.getError());
            }
        }

        return state;
    }

    public T getResult() {
        return task.getResult();
    }
    
    public PostgresqlException getError() {
        return task.getError();
    }

    public CommandStatus getCommandStatus() {
        return task.getCommandStatus();
    }

    public TransactionStatus getTransactionStatus() {
        return task.getTransactionStatus();
    }
    
    public TaskState onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        return process(task.onStart(fe, readBuffer));
    }
    
    public TaskState onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        return process(task.onRead(fe, readBuffer));
    }
    
    public TaskState onWrite(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        return process(task.onWrite(fe, readBuffer));
    }
    
    public void onFail(final Throwable t) {
        future.completeExceptionally(t);
    }
    
    public TaskState onTimeout(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        return process(task.onTimeout(fe, readBuffer));
    }

    public long getTimeout() {
        return task.getTimeout();
    }
    
    public TimeUnit getUnits() {
        return task.getUnits();
    }
    
    public void setOobHandlers(final Map<BackEnd,Consumer<Response>> oobHandlers) {
        task.setOobHandlers(oobHandlers);
    }
}
