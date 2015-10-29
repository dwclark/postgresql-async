package db.postgresql.async;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.CompletableFuture;
import static db.postgresql.async.TaskState.*;

public class Task <T> {
    
    private final CompletableFuture<T> future;
    public CompletableFuture<T> getFuture() { return future; }

    public Task() {
        this.future = new CompletableFuture<>();
    }

    public boolean holdsFullRecord(final ByteBuffer buffer) {
        //assumes that position is pointed at start of record
        if(buffer.remaining() < 8) {
            return false;
        }

        final int size = buffer.getInt(4);
        return (buffer.remaining() - 8) >= size;
    }

    public TaskState perform(final ByteBuffer buffer) {
        //TODO: add code to complete task if needed
        return finished();
    }
}
