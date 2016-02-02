package db.postgresql.async;

import db.postgresql.async.messages.KeyData;
import db.postgresql.async.pginfo.PgAttribute;
import db.postgresql.async.pginfo.PgType;
import db.postgresql.async.pginfo.PgTypeRegistry;
import db.postgresql.async.tasks.TransactionTask;
import db.postgresql.async.tasks.NotificationTask;
import db.postgresql.async.tasks.SimpleTask;
import db.postgresql.async.tasks.StartupTask;
import db.postgresql.async.tasks.TerminateTask;
import db.postgresql.async.messages.Notification;
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
import java.util.concurrent.CountDownLatch;
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
import java.util.function.Consumer;
import java.util.function.Function;
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


                IO io = startupIO();
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

        public void shutdown() {
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

        public final NotificationTask task;
        public volatile IO io;
        private volatile CountDownLatch latch;
        
        public Dedicated(final NotificationTask task) {
            this.task = task;
            recover();
        }
        
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
                io = startupIO();
                io.setPool(this).execute(task);
            }
            catch(IOException | InterruptedException | ExecutionException ex) {
                recoveryService.schedule(() -> recover(), sessionInfo.getBackOff(), sessionInfo.getBackOffUnits());
            }
        }

        public void bad(final IO io) {
            if(latch != null) {
                recover();
            }
            else {
                latch.countDown();
            }
        }

        public void shutdown() {
            try {
                latch = new CountDownLatch(1);
                task.shutdown();
                latch.await();
            }
            catch(InterruptedException e) {}
        }
    }

    private final SessionInfo sessionInfo;
    private final ExecutorService ioService;
    private final ScheduledExecutorService recoveryService;
    private final ExecutorService busyService;
    private final AsynchronousChannelGroup channelGroup;
    private final IOPool ioPool;
    private final Dedicated dedicatedPool;
    
    public Session(final SessionInfo sessionInfo) {
        try {
            this.sessionInfo = sessionInfo;
            this.recoveryService = Executors.newSingleThreadScheduledExecutor();
            this.ioService = new ThreadPoolExecutor(sessionInfo.getMinChannels(), sessionInfo.getMaxChannels(),
                                                    60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                                                    new PrefixFactory("Session-IO-Pool"));
            this.busyService = Executors.newSingleThreadExecutor(new PrefixFactory("Session-Busy-Pool"));
            this.channelGroup = AsynchronousChannelGroup.withThreadPool(ioService);
            this.ioPool = new IOPool();
            this.dedicatedPool = dedicatedPool();
        }
        catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Dedicated dedicatedPool() {
        if(sessionInfo.getNotifications()) {
            return new Dedicated(new NotificationTask(sessionInfo));
        }
        else {
            return null;
        }
    }

    private IO startupIO() throws IOException, InterruptedException, ExecutionException {
        final AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(channelGroup);
        channel.connect(sessionInfo.getSocketAddress()).get();
        final IO io = new IO(sessionInfo, channel);
        final CompletableTask<KeyData> startupTask = new StartupTask(sessionInfo);
        io.execute(startupTask);
        KeyData keyData = startupTask.getFuture().get();
        io.setKeyData(keyData);
        return io;
    }

    public SessionInfo getSessionInfo() {
        return sessionInfo;
    }

    public void shutdown() {
        ioPool.shutdown();
        if(dedicatedPool != null) {
            dedicatedPool.shutdown();
        }
    }

    public int getIoCount() {
        return ioPool.total;
    }

    public <T> CompletableFuture<T> execute(final CompletableTask<T> task) {
        if(task.isExecuted()) {
            throw new IllegalStateException("Task has already been executed");
        }

        task.executed();
        final IO io = ioPool.fast();
        if(io != null) {
            io.setPool(ioPool).execute(task);
        }
        else {
            busyService.execute(() -> ioPool.guaranteed().setPool(ioPool).execute(task));
        }
        
        return task.getFuture();
    }

    public <T> CompletableFuture<T> execute(final Task<T> task) {
        return execute(task.toCompletable());
    }

    public <T> CompletableFuture<T> call(final Task<T> task) {
        return execute(task.toCompletable());
    }
    
    public <T> CompletableFuture<T> call(final CompletableTask<T> task) {
        return execute(task);
    }

    public <T> CompletableFuture<T> call(final TransactionTask.Builder<T> builder) {
        return execute(builder.build());
    }

    public CompletableFuture<Void> listen(final String channel, final Consumer<Notification> consumer) {
        if(dedicatedPool == null) {
            throw new UnsupportedOperationException("Notifications are not configured for this session");
        }
        else {
            return dedicatedPool.task.add(channel, consumer);
        }
    }

    public CompletableFuture<Void> unlisten(final String channel) {
        if(dedicatedPool == null) {
            throw new UnsupportedOperationException("Notifications are not configured for this session");
        }
        else {
            return dedicatedPool.task.remove(channel);
        }
    }
}
