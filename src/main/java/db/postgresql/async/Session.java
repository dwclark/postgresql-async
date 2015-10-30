package db.postgresql.async;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Session {

    private class PrefixFactory implements ThreadFactory {

        private final AtomicInteger counter = new AtomicInteger(0);
        private final String prefix;

        public PrefixFactory(final String prefix) {
            this.prefix = prefix;
        }
        
        public String getNext() {
            return new StringBuilder(50)
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
        private final Lock lock = new ReentrantLock();
        private volatile int total = 0;

        private void add() {
            lock.lock();
            try {
                if(total == sessionInfo.getMaxChannels()) {
                    return;
                }

                final AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(channelGroup);
                channel.connect(sessionInfo.getSocketAddress()).get();
                final IO io = new IO(channel, this);
                ++total;
                queue.offer(io);
                semaphore.release();
            }
            catch(IOException | InterruptedException | ExecutionException ex) {
                recoveryService.schedule(() -> add(), sessionInfo.getBackOff(), sessionInfo.getBackOffUnits());
            }
            finally {
                lock.unlock();
            }
        }
        
        public IOPool() {
            for(int i = 0; i < sessionInfo.getMinChannels(); ++i) {
                add();
            }
        }
        
        public IO fast() {
            if(semaphore.tryAcquire()) {
                return queue.poll();
            }

            if(total < sessionInfo.getMaxChannels()) {
                recoveryService.submit(() -> add());
            }

            return null;
        }

        public IO guaranteed() {
            semaphore.acquireUninterruptibly();
            return queue.poll();
        }

        public void good(final IO io) {
            queue.offer(io);
        }

        public void bad(final IO io) {
            lock.lock();
            try {
                --total;
                if(total < sessionInfo.getMinChannels()) {
                    recoveryService.schedule(() -> add(), sessionInfo.getBackOff(), sessionInfo.getBackOffUnits());
                }
            }
            finally {
                lock.unlock();
            }
        }
    }

    private final SessionInfo sessionInfo;
    private final ExecutorService ioService;
    private final ExecutorService busyService;
    private final AsynchronousChannelGroup channelGroup;
    private final IOPool ioPool;
    
    public Session(final SessionInfo sessionInfo) {
        try {
            this.sessionInfo = sessionInfo;
            this.ioService = new ThreadPoolExecutor(sessionInfo.getMinChannels(), sessionInfo.getMaxChannels(),
                                                    60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                                                    new PrefixFactory("Session-IO-Pool"));
            this.busyService = Executors.newSingleThreadExecutor(new PrefixFactory("Session-Busy-Pool"));
            this.channelGroup = AsynchronousChannelGroup.withThreadPool(ioService);
            this.ioPool = new IOPool();
        }
        catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public <T> CompletableFuture<T> execute(final Task<T> task) {
        final IO io = ioPool.fast();
        if(io != null) {
            io.execute(task);
        }
        else {
            busyService.execute(() -> ioPool.guaranteed().execute(task));
        }
        
        return task.getFuture();
    }
}
