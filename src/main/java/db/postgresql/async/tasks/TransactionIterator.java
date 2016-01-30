package db.postgresql.async.tasks;

import db.postgresql.async.*;
import java.util.function.*;
import java.util.*;

public class TransactionIterator<T> implements TaskIterator<T> {

    public TransactionIterator(final Concurrency concurrency, final T accumulator,
                               final List<Function<T,Task<?>>> tasks) {
        this.accumulator = accumulator;
        decisions.add((accum) -> SimpleTask.begin(concurrency));
        decisions.addAll(tasks);
        decisions.add((accum) -> SimpleTask.commit());
    }

    private boolean issuedRollback = false;
    
    private final List<Function<T, Task<?>>> decisions = new ArrayList<>();
    private int index = 0;
    
    private final T accumulator;
    public T getAccumulator() { return accumulator; }

    public boolean hasNext(final Task<?> task) {
        if(task == null) {
            return true;
        }

        if(task.getError() != null && !issuedRollback) {
            decisions.clear();
            decisions.add((a) -> SimpleTask.rollback());
            index = 0;
            issuedRollback = true;
            return true;
        }

        return index < decisions.size();
    }

    public Task<?> next() {
        try {
            final Task<?> task = decisions.get(index++).apply(accumulator);
            if(task.isTerminal()) {
                index = decisions.size();
            }
            
            return task;
        }
        catch(Throwable t) {
            decisions.clear();
            final Task<?> task = SimpleTask.rollback();
            task.setError(t);
            issuedRollback = true;
            return task;
        }
    }
}
