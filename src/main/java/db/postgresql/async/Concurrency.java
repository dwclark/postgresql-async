package db.postgresql.async;

import db.postgresql.async.tasks.AnonymousTask;

public class Concurrency {
    
    private final Isolation isolation;
    private final RwMode mode;
    private final boolean deferrable;

    public Concurrency() {
        this(Isolation.READ_COMMITTED);
    }

    public Concurrency(final Isolation isolation) {
        this(isolation, RwMode.READ_WRITE);
    }

    public Concurrency(final Isolation isolation, final RwMode mode) {
        this(isolation, mode, false);
    }

    public Concurrency(final Isolation isolation, final RwMode mode, final boolean deferrable) {
        this.isolation = isolation;
        this.mode = mode;
        this.deferrable = deferrable;
    }
    public AnonymousTask<NullOutput> begin() {
        return AnonymousTask.begin(isolation, mode, deferrable);
    }
}
