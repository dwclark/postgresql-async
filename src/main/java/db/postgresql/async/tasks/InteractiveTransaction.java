package db.postgresql.async.tasks;

import db.postgresql.async.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;

public class InteractiveTransaction implements Transaction {

    final IO io;
    boolean completed = false;
    
    public InteractiveTransaction(final IO io, final Concurrency concurrency) {
        this.io = io;
        CompletableTask<NullOutput> begin = concurrency.begin().toCompletable();
        io.execute(begin);
        try {
            begin.getFuture().get();
        }
        catch(InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        if(completed) {
            return;
        }
        
        completed = true;
        guard(SimpleTask.commit().toCompletable());
    }

    public void rollback() {
        if(completed) {
            return;
        }
        
        completed = true;
        try {
            CompletableTask<NullOutput> task = SimpleTask.rollback().toCompletable();
            io.execute(task);
            task.getFuture().get();
        }
        catch(InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkTransactionStatus(final Task<?> task) {
        if(task.getTransactionStatus().isFailure()) {
            rollback();
            throw task.getError();
        }
    }

    private <T> Task<T> guard(final Task<T> task) {
        if(completed) {
            throw new RuntimeException("Transaction has completed, no future operations are permitted");
        }
        
        try {
            CompletableTask<T> ct = task.toCompletable();
            io.execute(ct);
            ct.getFuture().get();
        }
        catch(RuntimeException e) {
            rollback();
            throw e;
        }
        catch(InterruptedException | ExecutionException e) {
            rollback();
            throw new RuntimeException(e);
        }

        checkTransactionStatus(task);
        return task;
    }
    
    public int simple(final String sql) {
        return guard(Task.simple(sql)).getResult();
    }

    public <T> List<T> simple(final String sql, final Function<Row,T> processor) {
        return simple(sql, new ArrayList<>(), (list, row) -> {
                list.add(processor.apply(row));
                return list;
            });
    }

    public <T> T simple(final String sql, final T accumulator, final BiFunction<T,Row,T> processor) {
        return guard(Task.simple(sql, accumulator, processor)).getResult();
    }

    public List<Object> simple(final List<QueryPart<?>> parts) {
        return guard(Task.simple(parts)).getResult();
    }

    public void noOutput(final String sql) {
        guard(Task.noOutput(sql));
    }

    public int prepared(final String sql, final List<Object> args) {
        return guard(Task.prepared(sql, args)).getResult();
    }

    public List<Integer> bulkPrepared(final String sql, final List<List<Object>> args) {
        return guard(Task.bulkPrepared(sql, args)).getResult();
    }

    public <T> List<T> prepared(final String sql, final List<Object> args, final Function<Row,T> processor) {
        return guard(Task.prepared(sql, args, processor)).getResult();
    }
}
