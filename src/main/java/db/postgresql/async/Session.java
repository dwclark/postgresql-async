package db.postgresql.async;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class Session {

    private final SessionInfo sessionInfo;
    public SessionInfo getSessionInfo() { return sessionInfo; }

    private final Task<Void> defaultTask;
    private final ExecutorService ioPool;
    private final AsynchronousChannelGroup channelGroup;
    private Queue<IO> ioChannels = new ConcurrentLinkedQueue<>();
    
    private class IoPoolFactory implements ThreadFactory {

        private final AtomicInteger counter = new AtomicInteger(0);
        
        public String getNext() {
            return String.format("Session-IO-Pool-%s-%s-%d", sessionInfo.getDatabase(),
                                 sessionInfo.getUser(), counter.incrementAndGet());
        }
        
        public Thread newThread(Runnable r) {
            return new Thread(r, getNext());
        }
    }

    private Future<?> startNewIo() {
        Runnable r = () -> {
            InetSocketAddress addr = new InetSocketAddress(sessionInfo.getHost(), sessionInfo.getPort());
            ioChannels.offer(new IO(addr, channelGroup, ioPool, defaultTask));
        };
        
        return ioPool.submit(r);
    }
    
    public Session(final SessionInfo sessionInfo, final Task<Void> defaultTask) {
        try {
            this.sessionInfo = sessionInfo;
            this.defaultTask = defaultTask;
            this.ioPool = Executors.newCachedThreadPool(new IoPoolFactory());
            this.channelGroup = AsynchronousChannelGroup.withThreadPool(ioPool);

            ArrayList<Future<?>> futures = new ArrayList<>();
            for(int i = 0; i < sessionInfo.getChannels(); ++i) {
                futures.add(startNewIo());
            }

            for(Future<?> future : futures) {
                future.get();
            }
        }
        catch(IOException | InterruptedException | ExecutionException ex) {
            throw new RuntimeException(ex);
        }
    }

    public <T> CompletableFuture<T> execute(final ByteBuffer buffer, Task<T> task) {
        final IO io = ioChannels.poll();
        if(io.isOpen()) {
            CompletableFuture<T> future = io.execute(buffer, task);
            ioChannels.offer(io);
            return future;
        }
        else {
            startNewIo();
            return execute(buffer, task);
        }
    }
}
