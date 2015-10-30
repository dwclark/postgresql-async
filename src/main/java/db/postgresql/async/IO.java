package db.postgresql.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;

class IO {
    private final AsynchronousSocketChannel channel;
    private final ResourcePool<IO> resourcePool;
    private final Reader reader = new Reader();
    private final Writer writer = new Writer();
    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(32 * 1024);
    private ByteBuffer readBuffer = ByteBuffer.allocateDirect(32 * 1024);
    
    public IO(final AsynchronousSocketChannel channel, final ResourcePool<IO> resourcePool) {
        this.channel = channel;
        this.resourcePool = resourcePool;
    }
    
    public boolean isOpen() {
        return channel.isOpen();
    }
    
    private void close() {
        try {
            channel.close();
        }
        catch(IOException ex) { }
    }

    private class Base {
        public void failed(final Throwable ex, final Task task) {
            task.onFail(ex);
            close();
            resourcePool.bad(IO.this);
        }
    }

    private class Writer extends Base implements CompletionHandler<Integer,Task> {

        public void completed(final Integer bytes, final Task task) {
            if(writeBuffer.remaining() > 0) {
                channel.write(writeBuffer, task, writer);
            }
            else {
                writeBuffer.clear();
                final TaskState state = task.onWrite(writeBuffer, readBuffer);
                decide(state, task);
            }
        }
    }

    private class Reader extends Base implements CompletionHandler<Integer,Task> {

        public void completed(final Integer bytes, final Task task) {
            readBuffer.flip();
            final TaskState state = task.onRead(writeBuffer, readBuffer);
            readBuffer.compact();
            decide(state, task);
        }
    }
    
    private void increase(final int by) {
        final int position = readBuffer.position();
        final int limit = readBuffer.limit();
        final ByteBuffer tmp = ByteBuffer.allocateDirect(readBuffer.capacity() + by);
        tmp.put(readBuffer);
        tmp.limit(limit);
        tmp.position(position);
        readBuffer = tmp;
    }

    private int incrementBy(final int needed) {
        final int remainingCapacity = readBuffer.capacity() - readBuffer.position();
        return (needed - remainingCapacity);
    }

    private void decide(final TaskState state, final Task task) {
        if(state.next == TaskState.Next.READ) {
            if(state.needs > 0 && incrementBy(state.needs) > 0) {
                increase(incrementBy(state.needs));
            }
            
            channel.read(readBuffer, task, reader);
        }
        else if(state.next == TaskState.Next.WRITE) {
            channel.write(writeBuffer, task, writer);
        }
        else if(state.next == TaskState.Next.FINISHED) {
            resourcePool.good(this);
        }
        else {
            throw new UnsupportedOperationException(state.next + " is not a supported operation");
        }
    }
    
    public void execute(final Task task) {
        if(!channel.isOpen()) {
            task.getFuture().completeExceptionally(new ClosedChannelException());
            resourcePool.bad(this);
        }

        final TaskState state = task.onStart(writeBuffer, readBuffer);
        decide(state, task);
    }
}
