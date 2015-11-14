package db.postgresql.async;

import java.util.concurrent.CompletableFuture;
import java.util.ArrayList;
import java.util.List;

public abstract class Transaction<T> implements CompletableTask<T> {

    private final Isolation isolation;
    public Isolation getIsolation() { return isolation; }

    private final RwMode mode;
    private final RwMode getMode() { return mode; }

    private final boolean deferrable;
    private final boolean getDeferrable() { return deferrable; }

    private final List<Task<?>> tasks = new ArrayList<>();
    
    public Transaction() {
        this(Isolation.READ_COMMITTED);
    }

    public Transaction(final Isolation isolation) {
        this(isolation, RwMode.READ_WRITE);
    }

    public Transaction(final Isolation isolation, final RwMode mode) {
        this(isolation, mode, false);
    }
    
    public Transaction(final Isolation isolation, final RwMode mode, final boolean deferrable) {
        this.isolation = isolation;
        this.mode = mode;
        this.deferrable = deferrable;
    }

    public void add(final Task<?> task) {
        tasks.add(task);
    }
}
