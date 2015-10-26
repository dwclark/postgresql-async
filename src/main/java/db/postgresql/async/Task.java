package db.postgresql.async;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.CompletableFuture;
import static db.postgresql.async.TaskState.*;

class Task <T> implements Runnable {
    
    private final CompletableFuture<T> future;
    public CompletableFuture<T> getFuture() { return future; }
    
    public Task() {
        this.future = new CompletableFuture<>();
    }

    public TaskState perform(final ByteBuffer buffer) {
        return finished();
    }
    
    public void run() { }
}
