package db.postgresql.async;

import db.postgresql.async.CommandStatus;
import db.postgresql.async.PostgresqlException;
import db.postgresql.async.TransactionStatus;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.Response;
import db.postgresql.async.tasks.SimpleTask;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class Transaction<T> implements CompletableTask<T> {

    private Transaction(SimpleTask<NullOutput> beginTask, final List<Task> tasks) {
        this.current = beginTask;
        this.tasks = tasks;
        this.phase = Phase.BEGIN;
    }

    private enum Phase { BEGIN, RUN, COMMIT, ROLLBACK };

    private final CompletableFuture<T> future = new CompletableFuture<>();
    private final List<Task> tasks;
    private Phase phase;
    private Map<BackEnd,Consumer<Response>> oobHandlers;
    private Iterator<Task> iter;
    private Task<?> current;
    private TransactionStatus transactionStatus;
    private CommandStatus commandStatus;
    private PostgresqlException error;
    private TaskState nextState;

    public CompletableFuture<T> getFuture() {
        return future;
    }
    
    public TransactionStatus getTransactionStatus() {
        return transactionStatus;
    }

    public CommandStatus getCommandStatus() {
        return commandStatus;
    }

    public PostgresqlException getError() {
        return error;
    }

    abstract protected void onCompleteTask(Task<?> task);

    private void advanceEnd() {
        while(iter.hasNext()) {
            iter.next();
        }
    }

    private void handleFail() {
        //current task has failed, begin rollback
        error = current.getError();
        advanceEnd();
        phase = Phase.ROLLBACK;
        current = SimpleTask.rollback();
        nextState = TaskState.start();
    }

    private void advanceNext() {
        if(phase == Phase.BEGIN) {
            iter = tasks.iterator();
            current = iter.next();
            nextState = TaskState.start();
            phase = Phase.RUN;
        }
        else if(phase == Phase.RUN) {
            nextState = TaskState.start();
            onCompleteTask(current);
            if(iter.hasNext()) {
                current = iter.next();
            }
            else {
                phase = Phase.COMMIT;
                current = SimpleTask.commit();
                iter = null;
            }
        }
        else if(phase == Phase.COMMIT) {
            nextState = TaskState.finished();
            future.complete(getResult());
        }
        else if(phase == Phase.ROLLBACK) {
            nextState = TaskState.finished();
            future.completeExceptionally(error);
        }
    }

    private void computeNextAction() {
        if(current.getTransactionStatus() == null) {
            nextState = current.getNextState();
        }
        else if(current.getTransactionStatus() == TransactionStatus.FAILED) {
            handleFail();
        }
        else if(current.getTransactionStatus() == TransactionStatus.IN_BLOCK) {
            advanceNext();
        }
        else {
            throw new IllegalStateException("Transaction status should never be " + current.getTransactionStatus() +
                                            "inside a transaction block");
        }
    }

    public void onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        current.setOobHandlers(oobHandlers);
        current.onStart(fe, readBuffer);
        computeNextAction();
    }
    
    public void onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        current.onRead(fe, readBuffer);
        computeNextAction();
    }
    
    public void onWrite(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        current.onWrite(fe, readBuffer);
        computeNextAction();
    }
    
    public void onFail(final Throwable t) {
        nextState = TaskState.terminate();
        future.completeExceptionally(t);
    }
    
    public void onTimeout(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        current.onTimeout(fe, readBuffer);
        computeNextAction();
    }
    
    public TaskState getNextState() {
        return nextState;
    }

    public long getTimeout() {
        if(current != null) {
            return current.getTimeout();
        }
        else {
            return 0L;
        }
    }

    public TimeUnit getUnits() {
        if(current != null) {
            return current.getUnits();
        }
        else {
            return TimeUnit.SECONDS;
        }
    }

    public void setOobHandlers(final Map<BackEnd,Consumer<Response>> oobHandlers) {
        this.oobHandlers = oobHandlers;
    }

    private static class Single<T> extends Transaction<T> {

        private Task<T> theTask;

        public T getResult() {
            return theTask.getResult();
        }
        
        public Single(final SimpleTask<NullOutput> begin, final Task<T> task) {
            super(begin, Collections.singletonList(task));
            this.theTask = task;
        }

        protected void onCompleteTask(final Task task) {
            //do nothing, not needed
        }
    }

    private static class Multiple extends Transaction<List> {
        private List result = new ArrayList<>();

        public List getResult() {
            return result;
        }

        public Multiple(final SimpleTask<NullOutput> begin, final List<Task> tasks) {
            super(begin, tasks);
        }

        @SuppressWarnings("unchecked")
        protected void onCompleteTask(final Task task) {
            result.add(task.getResult());
        }
    }
    
    public static class Builder {
        private Isolation isolation = Isolation.READ_COMMITTED;
        public Builder isolation(final Isolation val) { this.isolation = val; return this; }

        private RwMode mode = RwMode.READ_WRITE;
        public Builder mode(final RwMode val) { this.mode = val; return this; }

        private boolean deferrable = false;
        public Builder deferrable(final boolean val) { this.deferrable = val; return this; }

        private List<Task> tasks = new ArrayList<>();
        public Builder task(final Task task) { tasks.add(task); return this; }
        public List<Task> tasks() {
            if(tasks.size() == 1) {
                return Collections.singletonList(tasks.get(0));
            }
            else {
                return Collections.unmodifiableList(tasks);
            }
        }

        public SimpleTask<NullOutput> begin() {
            return SimpleTask.begin(isolation, mode, deferrable);
        }

        public <T> Transaction<T> single(final Task<T> task) {
            return new Single<>(begin(), task);
        }

        public Transaction<List> multiple() {
            return new Multiple(begin(), tasks());
        }
    }
}
