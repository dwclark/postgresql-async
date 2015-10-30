package db.postgresql.async;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public abstract class Task <T> {
    
    private final CompletableFuture<T> future;
    public CompletableFuture<T> getFuture() { return future; }

    public Task() {
        this.future = new CompletableFuture<>();
    }

    public abstract TaskState onStart(ByteBuffer writeBuffer, ByteBuffer readBuffer);
    public abstract TaskState onRead(ByteBuffer writeBuffer, ByteBuffer readBuffer);
    public abstract TaskState onWrite(ByteBuffer writeBuffer, ByteBuffer readBuffer);

    public void onFail(Throwable t) {
        future.completeExceptionally(t);
    }
}
