package db.postgresql.async;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

public class Session {

    private static class PrefixFactory implements ThreadFactory {

        private final AtomicInteger counter = new AtomicInteger(0);
        private final String prefix;

        public PrefixFactory(final String prefix) {
            this.prefix = prefix;
        }
        
        public String getNext() {
            new StringBuilder(50)
                .append(prefix).append("-")
                .append(sessionInfo.getDatabase()).append("-")
                .append(sessionInfo.getUser()).append("-")
                .append(counter.incrementAndGet()).toString();
        }
        
        public Thread newThread(Runnable r) {
            return new Thread(r, getNext());
        }
    }

    private class IOPool implements ResourcePool<IO> {
        private final ScheduledExecutorService recoveryService = Executors.newSingleThreadScheduledExecutor();
        private final Queue<IO> queue = new ConcurrentLinkedQueue<>();
        private final Semaphore semaphore = new Semaphore(0);
        private final ReentrantLock totalLock = new ReentrantLock();
        private volatile int total = 0;

        private void add() {
            total.lock();
            try {
                if(total == sessionInfo.getMaxChannels()) {
                    return;
                }

                final IO io = new IO(sessionInfo.getSocketAddress(), channelGroup, ioService);
                ++total;
                queue.put(io);
                semaphore.release();
            }
            finally {
                totalLock.unlock();
            }
        }
        
        public IOPool() {
            for(int i = 0; i < sessionInfo.getMinChannels(); ++i) {
                add();
            }
        }
        
        public IO fast() {
            if(semaphore.tryAquire()) {
                return queue.poll();
            }

            if(total < sessionInfo.getMaxChannels()) {
                recoveryService.submit(() -> add());
            }

            return null;
        }

        public IO guaranteed() {
            semaphore.aqcuireUninterruptibly();
            return queue.poll();
        }

        public void good(final IO io) {
            queue.put(io);
        }

        public void bad(final IO io) {
            totalLock.lock();
            try {
                --total;
                if(total < sessionInfo.getMinChannels()) {
                    recoveryService.schedule(() -> add(), sessionInfo.getBackOff(), sessionInfo.getBackOffUnits());
                }
            }
            finally {
                totalLock.unlock();
            }
        }
    }

    private final SessionInfo sessionInfo;
    public SessionInfo getSessionInfo() { return sessionInfo; }

    private final Task<Void> defaultTask;
    private final ExecutorService ioService;
    private final ExecutorService busyService;
    private final AsynchronousChannelGroup channelGroup;
    private final IOPool ioPool;
    
    public Session(final SessionInfo sessionInfo) {
        try {
            this.sessionInfo = sessionInfo;
            this.ioPool = new IOPool();
            this.ioService = Executors.newCachedThreadPool(new PoolFactory("Session-IO-Pool"));
            this.busyService = Executors.newSingleThreadedExecutor(new PoolFactory("Session-Busy-Pool"));
            this.channelGroup = AsynchronousChannelGroup.withThreadPool(ioService);
        }
        catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public <T> CompletableFuture<T> execute(final ByteBuffer buffer, Task<T> task) {
        final IO io = ioPool.fast();
        if(io != null) {
            io.execute(buffer, task);
        }
        else {
            busyService.execute(() -> ioPool.guaranteed().execute(buffer, task));
        }
        
        return task.getFuture();
    }
}
