package db.postgresql.async;

import java.util.concurrent.CompletableFuture;

public interface CompletableTask<T> extends Task<T> {
    CompletableFuture<T> getFuture();
}
