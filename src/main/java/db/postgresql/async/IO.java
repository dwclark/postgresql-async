package db.postgresql.async;

import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.KeyData;
import db.postgresql.async.messages.Notice;
import db.postgresql.async.messages.Notification;
import db.postgresql.async.messages.ParameterStatus;
import db.postgresql.async.messages.Response;
import db.postgresql.async.pginfo.PgSessionCache;
import db.postgresql.async.serializers.SerializationContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class IO {

    public static interface IOCompletion {
        void success();
        void failure();
    }

    public IO onCompleteToPool(final ResourcePool<IO> pool) {
        ioCompletionHandler = new IOCompletion() {
                public void success() { pool.good(IO.this); }
                public void failure() { pool.bad(IO.this); }
            };
        return this;
    }

    private static final IOCompletion DO_NOTHING = new IOCompletion() {
            public void success() { }
            public void failure() { }
        };
    
    public IO onCompleteDoNothing() {
        ioCompletionHandler = DO_NOTHING;
        return this;
    }
    
    private final SessionInfo sessionInfo;
    private final FrontEndMessage feMessage;
    private final AsynchronousSocketChannel channel;
    private final PgSessionCache pgSessionCache = new PgSessionCache();
    private final Reader reader = new Reader();
    private final Writer writer = new Writer();
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(32 * 1024);
    private ByteBuffer readBuffer = ByteBuffer.allocate(32 * 1024);
    private final Map<String,String> parameterStatuses = new LinkedHashMap<>();
    private Notice lastNotice;
    private final Map<BackEnd,Consumer<Response>> oobHandlers = new EnumMap<>(BackEnd.class);
    private KeyData keyData;
    private IOCompletion ioCompletionHandler = DO_NOTHING;

    public void setKeyData(final KeyData val) {
        this.keyData = val;
    }

    public KeyData getKeyData() {
        return keyData;
    }

    //OOB handlers
    public void handleParameterStatus(final Response r) {
        final ParameterStatus pstatus = (ParameterStatus) r;
        parameterStatuses.put(pstatus.getName(), pstatus.getValue());
    }

    public void handleNotice(final Response notice) {
        lastNotice = (Notice) notice;
    }
    
    private static final Consumer<Response> failNotification = (notification) -> {
        throw new UnsupportedOperationException("IO channel does not support notifications. You need to allocate " +
                                                "a dedicated channel for notifications in the session");
    };

    public IO(final SessionInfo sessionInfo, final AsynchronousSocketChannel channel) {
        this(sessionInfo, channel, failNotification);
    }
    
    public IO(final SessionInfo sessionInfo, final AsynchronousSocketChannel channel,
              final Consumer<Response> handleNotifications) {
        this.sessionInfo = sessionInfo;
        this.feMessage = new FrontEndMessage(sessionInfo.getEncoding());
        feMessage.buffer = writeBuffer;
        this.channel = channel;
        oobHandlers.put(BackEnd.NoticeResponse, this::handleNotice);
        oobHandlers.put(BackEnd.NotificationResponse, handleNotifications);
        oobHandlers.put(BackEnd.ParameterStatus, this::handleParameterStatus);
    }

    public SessionInfo getSessionInfo() {
        return sessionInfo;
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
        public void failed(final Throwable ex, final CompletableTask task) {
            if(ex instanceof InterruptedByTimeoutException) {
                task.onTimeout(feMessage, readBuffer);
                decide(task);
            }
            else {
                task.onFail(ex);
                close();
                ioCompletionHandler.failure();
            }
        }
    }

    private void prepareThread() {
        SerializationContext.io(this);
    }
    
    private class Writer extends Base implements CompletionHandler<Integer,CompletableTask> {

        public void completed(final Integer bytes, final CompletableTask task) {
            if(writeBuffer.hasRemaining()) {
                channel.write(writeBuffer, task.getTimeout(), task.getUnits(), task, writer);
            }
            else {
                writeBuffer.clear();
                prepareThread();
                task.onWrite(feMessage, readBuffer);
                decide(task);
            }
        }
    }

    private class Reader extends Base implements CompletionHandler<Integer,CompletableTask> {

        public void completed(final Integer bytes, final CompletableTask task) {
            readBuffer.flip();
            prepareThread();
            task.onRead(feMessage, readBuffer);
            readBuffer.compact();
            decide(task);
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

    private void decide(final CompletableTask task) {
        prepareThread();
        final TaskState state = task.getNextState();
        if(state.next == TaskState.Next.START) {
            readBuffer.clear();
            writeBuffer.clear();
            task.onStart(feMessage, readBuffer);
            decide(task);
        }
        else if(state.next == TaskState.Next.READ) {
            if(state.needs > 0 && incrementBy(state.needs) > 0) {
                increase(incrementBy(state.needs));
            }

            channel.read(readBuffer, task.getTimeout(), task.getUnits(), task, reader);
        }
        else if(state.next == TaskState.Next.WRITE) {
            writeBuffer.flip();
            channel.write(writeBuffer, task.getTimeout(), task.getUnits(), task, writer);
        }
        else if(state.next == TaskState.Next.FINISHED) {
            ioCompletionHandler.success();
        }
        else if(state.next == TaskState.Next.TERMINATE) {
            close();
        }
        else {
            throw new UnsupportedOperationException(state.next + " is not a supported operation");
        }
    }
    
    public <T> void execute(final CompletableTask<T> task) {
        if(!channel.isOpen()) {
            task.getFuture().completeExceptionally(new ClosedChannelException());
            ioCompletionHandler.failure();
        }

        readBuffer.clear();
        writeBuffer.clear();
        task.setOobHandlers(oobHandlers);
        task.setStatementCache(pgSessionCache);
        prepareThread();
        task.onStart(feMessage, readBuffer);
        decide(task);
    }
}
