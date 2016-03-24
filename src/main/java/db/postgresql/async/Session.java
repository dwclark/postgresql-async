package db.postgresql.async;

import db.postgresql.async.serializers.SerializationContext;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
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
        private final BlockingQueue<IO> queue = new PriorityBlockingQueue<>();
        private final AtomicInteger total = new AtomicInteger();
        private volatile boolean shuttingDown = false;
        
        private void add() {
            if(shuttingDown) {
                return;
            }

            int tmpTotal = total.get();
            if(tmpTotal >= sessionInfo.getMaxChannels()) {
                return;
            }
            
            try {
                if(total.compareAndSet(tmpTotal, tmpTotal + 1)) {
                    IO io = startupIO(this);
                }
            }
            catch(IOException | InterruptedException | ExecutionException ex) {
                total.decrementAndGet();
                scheduler.schedule(() -> add(), sessionInfo.getBackOff(), sessionInfo.getBackOffUnits());
            }
        }

        public void shutdown() {
            shuttingDown = true;
            while(total.get() != 0) {
                try {
                    //Set timeout to not wait forever. It is possible that add() or
                    //bad() operations were executed between shuttingDown was set to
                    //false and we last got the total. This gives us a chance
                    //to recount and decide if we need to continue.
                    final IO io = queue.poll(100L, TimeUnit.MILLISECONDS);
                    if(io == null) {
                        continue;
                    }

                    if(!io.isOpen()) {
                        //bad() returned after shutdown
                        total.decrementAndGet();
                        continue;
                    }
                    
                    final CompletableTask<Void> task = new TerminateTask().toCompletable();
                    io.execute(task);
                    total.decrementAndGet();
                }
                catch(InterruptedException ie) {
                    //swallow and try again
                }
            }
        }
        
        public IOPool() {
            for(int i = 0; i < sessionInfo.getMinChannels(); ++i) {
                add();
            }
        }

        private RuntimeException shutdownError() {
            return new RuntimeException("Session is shutting down, only in flight transactions will complete");
        }
        
        public IO fast() {
            if(shuttingDown) {
                shutdownError();
            }
            
            final IO io = queue.poll();
            if(io != null) {
                return io;
            }
            
            if(total.get() < sessionInfo.getMaxChannels()) {
                scheduler.submit(() -> add());
            }

            return null;
        }

        public IO guaranteed() {
            if(shuttingDown) {
                throw shutdownError();
            }
            else {
                try {
                    return queue.take();
                }
                catch(InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
        }
        
        public void good(final IO io) {
            try {
                queue.put(io);
            }
            catch(InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }

        public void bad(final IO io) {
            if(shuttingDown) {
                //put it back in the pool, the cleanup operation won't re-close
                //it but it needs to reclaim all outstanding io objects.
                try {
                    queue.put(io);
                }
                catch(InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
                
                return;
            }

            total.decrementAndGet();
            scheduler.schedule(() -> add(), sessionInfo.getBackOff(), sessionInfo.getBackOffUnits());
        }
    }

    private class Dedicated implements ResourcePool<IO> {

        public final NotificationTask task;
        public volatile IO io;
        private volatile CountDownLatch latch;
        
        public Dedicated(final NotificationTask task) {
            this.task = task;
        }
        
        public IO fast() {
            throw new UnsupportedOperationException("You can't acquire IO channels from the dedicated pool");
        }
        
        public IO guaranteed() {
            return fast();
        }
        
        public void good(final IO o) {
            if(latch == null) {
                final Runnable r = () -> io.setPool(this).execute(task);
                scheduler.schedule(r,
                                   sessionInfo.getNotificationsTimeout(),
                                   sessionInfo.getNotificationsUnits());
            }
            else {
                latch.countDown();
            }
        }

        private void recover() {
            try {
                this.io = startupIO(this);
            }
            catch(IOException | InterruptedException | ExecutionException ex) {
                scheduler.schedule(() -> recover(), sessionInfo.getBackOff(), sessionInfo.getBackOffUnits());
            }
        }

        public void bad(final IO io) {
            if(latch == null) {
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

    private static class SessionExecutorService extends ThreadPoolExecutor {

        public SessionExecutorService() {
            super(sessionInfo.getMinChannels(), sessionInfo.getMaxChannels(),
                  60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                  new PrefixFactory("Session-IO-Pool"));
        }

        @Override
        protected void beforeExecute(final Thread t, final Runnable r) {
            SerializationContext.setup(sessionInfo);
            super.beforeExecute(t, r);
        }

        @Override
        protected void afterExecute(final Thread t, final Runnable r) {
            super.afterExecute(t, r);
            SerializationContext.cleanup();
        }
    }

    private final SessionInfo sessionInfo;
    private final ExecutorService ioService;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService busyService;
    private final AsynchronousChannelGroup channelGroup;
    private final IOPool ioPool;
    private final Dedicated dedicatedPool;
    
    public Session(final SessionInfo sessionInfo) {
        try {
            this.sessionInfo = sessionInfo;
            this.scheduler = Executors.newScheduledThreadPool(scheduledThreadCount(sessionInfo));
            this.ioService = new SessionExecutorService();
            this.busyService = Executors.newSingleThreadExecutor(new PrefixFactory("Session-Busy-Pool"));
            this.channelGroup = AsynchronousChannelGroup.withThreadPool(ioService);
            this.ioPool = new IOPool();
            this.dedicatedPool = dedicatedPool();
        }
        catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static int scheduledThreadCount(final SessionInfo sessionInfo) {
        if(sessionInfo.getNotifications()) {
            return 2;
        }
        else {
            return 1;
        }
    }

    private Dedicated dedicatedPool() {
        if(sessionInfo.getNotifications()) {
            Dedicated d =  new Dedicated(new NotificationTask());
            d.recover();
            return d;
        }
        else {
            return null;
        }
    }

    private IO startupIO(final ResourcePool<IO> pool) throws IOException, InterruptedException, ExecutionException {
        final AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(channelGroup);
        channel.connect(sessionInfo.getSocketAddress()).get();
        final IO io = new IO(sessionInfo, channel);
        final CompletableTask<KeyData> startupTask = new StartupTask(sessionInfo);
        io.setPool(pool).execute(startupTask);
        KeyData keyData = startupTask.getFuture().get();
        io.setKeyData(keyData);
        return io;
    }

    public SessionInfo getSessionInfo() {
        return sessionInfo;
    }

    public void shutdown() {
        scheduler.shutdown();
        ioPool.shutdown();
        if(dedicatedPool != null) {
            dedicatedPool.shutdown();
        }
    }

    public int getIoCount() {
        return ioPool.total.get();
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
