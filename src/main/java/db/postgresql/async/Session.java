package db.postgresql.async;

import db.postgresql.async.messages.KeyData;
import db.postgresql.async.pginfo.PgAttribute;
import db.postgresql.async.pginfo.PgType;
import db.postgresql.async.pginfo.PgTypeRegistry;
import db.postgresql.async.tasks.NotificationTask;
import db.postgresql.async.tasks.SimpleTask;
import db.postgresql.async.tasks.StartupTask;
import db.postgresql.async.tasks.TerminateTask;
import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;
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
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

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
        private final Queue<IO> queue = new ConcurrentLinkedQueue<>();
        private final Semaphore semaphore = new Semaphore(0);
        private final Lock lock = new ReentrantLock();
        private volatile int total = 0;
        private volatile boolean shuttingDown = false;
        
        private void add() {
            lock.lock();
            try {
                if(shuttingDown || total == sessionInfo.getMaxChannels()) {
                    return;
                }


                IO io = startupIO(this);
                ++total;
                good(io);
            }
            catch(IOException | InterruptedException | ExecutionException ex) {
                recoveryService.schedule(() -> add(), sessionInfo.getBackOff(), sessionInfo.getBackOffUnits());
            }
            finally {
                lock.unlock();
            }
        }

        private void shutdown() {
            shuttingDown = true;
            lock.lock();
            IntStream
                .range(0, total).mapToObj( (i) -> {
                        final IO io = guaranteed();
                        final CompletableTask<Void> task = new TerminateTask().toCompletable();
                        io.execute(task);
                        return task.getFuture();
                    })
                .forEach((future) -> {
                        try {
                            future.get();
                            --total;
                        }
                        catch(InterruptedException | ExecutionException e) {}
                    });
            
            lock.unlock();
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
            semaphore.release();
        }

        public void bad(final IO io) {
            lock.lock();
            try {
                if(shuttingDown) {
                    return;
                }
                
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

    private class Dedicated implements ResourcePool<IO> {
        public IO fast() {
            throw new UnsupportedOperationException("You can't acquire IO channels from the dedicated pool");
        }
        
        public IO guaranteed() {
            return fast();
        }
        
        public void good(final IO o) {
            //NO-OP
        }

        private void recover() {
            try {
                final IO io = startupIO(this);
                final NotificationTask notificationTask = notificationSupplier.get();
                io.execute(notificationTask);
            }
            catch(IOException | InterruptedException | ExecutionException ex) {
                recoveryService.schedule(() -> recover(), sessionInfo.getBackOff(), sessionInfo.getBackOffUnits());
            }
        }

        public void bad(final IO io) {
            recover();
        }
    }

    private final SessionInfo sessionInfo;
    private final ExecutorService ioService;
    private final ScheduledExecutorService recoveryService;
    private final ExecutorService busyService;
    private final AsynchronousChannelGroup channelGroup;
    private final IOPool ioPool;
    private final Supplier<NotificationTask> notificationSupplier;
    
    public Session(final SessionInfo sessionInfo, final Supplier<NotificationTask> notificationSupplier) {
        try {
            this.sessionInfo = sessionInfo;
            this.notificationSupplier = notificationSupplier;
            this.recoveryService = Executors.newSingleThreadScheduledExecutor();
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

    private IO startupIO(final ResourcePool<IO> pool) throws IOException, InterruptedException, ExecutionException {
        final AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(channelGroup);
        channel.connect(sessionInfo.getSocketAddress()).get();
        final IO io = new IO(sessionInfo, channel, pool);
        final CompletableTask<KeyData> startupTask = new StartupTask(sessionInfo);
        io.execute(startupTask);
        KeyData keyData = startupTask.getFuture().get();
        io.setKeyData(keyData);
        io.setInitialized(true);
        return io;
    }

    public SessionInfo getSessionInfo() {
        return sessionInfo;
    }

    public void shutdown() {
        ioPool.shutdown();
    }

    public int getIoCount() {
        return ioPool.total;
    }

    public <T> CompletableFuture<T> execute(final CompletableTask<T> task) {
        final IO io = ioPool.fast();
        if(io != null) {
            io.execute(task);
        }
        else {
            busyService.execute(() -> ioPool.guaranteed().execute(task));
        }
        
        return task.getFuture();
    }

    public CompletableFuture<Integer> execute(final String sql) {
        return execute(SimpleTask.execute(sql).toCompletable());
    }

    public <T> CompletableFuture<T> query(final String sql, final T accumulator,
                                          final BiFunction<T,Row,T> func) {
        return execute(SimpleTask.query(sql, accumulator, func).toCompletable());
    }
}
