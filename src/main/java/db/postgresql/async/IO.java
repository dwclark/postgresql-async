package db.postgresql.async;

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
    private static final long WRITE_TIMEOUT = 10L;
    private static final TimeUnit WRITE_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final ByteBuffer[] EMPTY_BUFFERS = new ByteBuffer[0];
    private final ExecutorService ioPool;
    private final AsynchronousSocketChannel channel;
    private final SocketAddress address;
    private final Queue<ByteBuffer> writes = new ConcurrentLinkedQueue<>();
    private final Queue<Task> tasks = new ConcurrentLinkedQueue<>();
    private final Task defaultTask;
    private final Semaphore writeLock = new Semaphore(1, true);
    private final Writer writer = new Writer();
    private final Reader reader = new Reader();
    private ByteBuffer readBuffer = ByteBuffer.allocate(32 * 1024);
    
    public IO(final SocketAddress address, final AsynchronousChannelGroup group,
              final ExecutorService ioPool, final Task defaultTask) {
        try {
            this.address = address;
            this.channel = AsynchronousSocketChannel.open(group);
            this.ioPool = ioPool;
            this.defaultTask = defaultTask;
            channel.connect(address).get();
            channel.read(readBuffer, reader, null);
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

    private void failAllTasks(Throwable t) {
        Task task = tasks.poll();
        while(task != null) {
            task.getFuture().completeExceptionally(t);
            task = tasks.poll();
        }
    }

    private Task getCurrentTask() {
        Task first = tasks.peek();
        return (first != null) ? first : defaultTask;            
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

    private class Writer implements CompletionHandler<Long, Void>, Runnable {

        private ByteBuffer[] next() {
            ByteBuffer next = writes.peek();
            while(next != null && next.remaining() == 0) {
                writes.poll();
                next = writes.peek();
            }
            
            return writes.toArray(EMPTY_BUFFERS);
        }

        private void doWrite() {
            ByteBuffer[] next = next();
            if(next.length > 0) {
                channel.write(next, 0, next.length, WRITE_TIMEOUT, WRITE_TIMEOUT_UNIT,
                              null, this);
            }
            else {
                writeLock.release();
            }
        }

        public void start() {
            boolean locked = writeLock.tryAcquire();
            if(locked) {
                doWrite();
            }
            else {
                ioPool.execute(this);
            }
        }

        public void run() {
            writeLock.acquireUninterruptibly();
            doWrite();
        }
        
        public void completed(Long bytes, Void v) {
            doWrite();
        }

        public void failed(Throwable ex, Void v) {
            writeLock.release();
            close();
            failAllTasks(ex);
        }
    }

    private class Reader implements CompletionHandler<Integer,Void> {

        public void completed(Integer bytes, Void v) {
            readBuffer.flip();
            TaskState state = getCurrentTask().perform(readBuffer);
            readBuffer.compact();
            if(state.next == TaskState.Next.FINISHED) {
                tasks.poll();
            }
            else if(state.next == TaskState.Next.AT_LEAST) {
                final int possible = readBuffer.capacity() - readBuffer.position();
                final int needed = state.bytes - possible;
                if(needed > 0) {
                    increase(needed);
                }
            }

            channel.read(readBuffer, this, null);
        }
        
        public void failed(Throwable ex, Void v) {
            close();
            failAllTasks(ex);
        }
    }

    public <T> CompletableFuture<T> execute(final ByteBuffer buffer, final Task<T> task) {
        if(!channel.isOpen()) {
            task.getFuture().completeExceptionally(new ClosedChannelException());
            return task.getFuture();
        }
        
        tasks.offer(task);
        writes.offer(buffer);
        writer.start();
        return task.getFuture();
    }
}
