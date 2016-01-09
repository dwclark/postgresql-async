package db.postgresql.async.tasks;

import db.postgresql.async.Task;
import db.postgresql.async.Concurrency;
import java.util.List;
import java.util.ArrayList;
import java.util.function.Function;

public class TransactionTask<T> extends MultiStageTask<T> {

    public TransactionTask(final TransactionIterator<T> iter) {
        super(iter);
    }

    public static class Builder<T> {
        private T accumulator = null;
        public Builder<T> accumulator(final T val) { accumulator = val; return this; }

        private Concurrency concurrency = new Concurrency();
        public Builder<T> concurrency(final Concurrency val) { concurrency = val; return this; }

        private List<Function<T,Task<?>>> stages;
        public Builder<T> then(final Function<T,Task<?>> f) { stages.add(f); return this; }
        public Builder<T> leftShift(final Function<T,Task<?>> f) { return then(f); }
        
        public Builder() {
            this.stages = new ArrayList<>();
        }

        public TransactionTask<T> build() {
            final TransactionIterator<T> iter = new TransactionIterator<>(concurrency, accumulator, stages);
            return new TransactionTask<>(iter);
        }
    }
}
