package db.postgresql.async;

import java.util.Map;
import java.util.AbstractMap;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

class IO {
    private final AsynchronousSocketChannel channel;
    private final ResourcePool resourcePool;
    private final Reader reader = new Reader();
    private final Writer writer = new Writer();
    private ByteBuffer readBuffer = ByteBuffer.allocate(32 * 1024);
    
    public IO(final SocketAddress address, final AsynchronousChannelGroup group, final ResourcePool resourcePool) {
        try {
            this.channel = AsynchronousSocketChannel.open(group);
            this.defaultTask = defaultTask;
            channel.connect(address).get();
            this.resourcePool = resourcePool;
        }
        catch(IOException | InterruptedException | ExecutionException ex) {
            throw new RuntimeException(ex);
        }
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

    public void increase(final int by) {
        final int position = readBuffer.position();
        final int limit = readBuffer.limit();
        final ByteBuffer tmp = ByteBuffer.allocate(readBuffer.capacity() + by);
        tmp.put(readBuffer);
        tmp.limit(limit);
        tmp.position(position);
        readBuffer = tmp;
    }

    private class Writer implements CompletionHandler<Integer, Map.Entry<ByteBuffer,Task>>, Runnable {

        public void completed(Integer bytes, Map.Entry<ByteBuffer,Task> pair) {
            if(pair.getKey().remaining()) {
                channel.write(pair.getKey(), pair, writer);
            }

            channel.read(readBuffer, pair.getValue(), reader);
        }

        public void failed(Throwable ex, Map.Entry<ByteBuffer,Task>) {
            task.getFuture().completeExceptionally(ex);
            close();
            resourcePool.bad(IO.this);
        }
    }

    private class Reader implements CompletionHandler<Integer,Task> {

        public void completed(Integer bytes, Task task) {
            readBuffer.flip();
            TaskState state = task.perform(readBuffer);
            readBuffer.compact();

            if(state.isFinished()) {
                return;
            }
            
            if(state.isAtLeast()) {
                final int possible = readBuffer.capacity() - readBuffer.position();
                final int needed = state.getBytes() - possible;
                if(needed > 0) {
                    increase(needed);
                }
            }

            if(state.isMore()) {
                channel.read(readBuffer, task, reader);
            }

            if(state.isWrite()) {
                channel.write(state.getByteBuffer(), task, reader);
            }
        }
        
        public void failed(Throwable ex, Task task) {
            task.getFuture().completeExceptionally(ex);
            close();
            resourcePool.bad(IO.this);
        }
    }

    public void execute(final ByteBuffer buffer, final Task task) {
        if(!channel.isOpen()) {
            task.getFuture().completeExceptionally(new ClosedChannelException());
            resourcePool.bad(this);
        }
        else {
            channel.write(buffer, task, writer);
        }
    }
}
