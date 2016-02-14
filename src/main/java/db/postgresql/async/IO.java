package db.postgresql.async;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import db.postgresql.async.buffers.BufferOps;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.KeyData;
import db.postgresql.async.messages.Notice;
import db.postgresql.async.messages.ParameterStatus;
import db.postgresql.async.messages.Response;
import db.postgresql.async.pginfo.StatementCache;
import db.postgresql.async.serializers.SerializationContext;
import db.postgresql.async.tasks.SslTask;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.nio.channels.ClosedChannelException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.io.EOFException;
public class IO {

    private final SessionInfo sessionInfo;
    private final FrontEndMessage feMessage;
    private final AsynchronousSocketChannel channel;
    private final StatementCache statementCache = new StatementCache();
    private final ByteBuffer writeBuffer;
    private ByteBuffer readBuffer;
    private final Map<String,String> parameterStatuses = new LinkedHashMap<>();
    private Notice lastNotice;
    private final Map<BackEnd,Consumer<Response>> oobHandlers = new EnumMap<>(BackEnd.class);
    private KeyData keyData;
    private ResourcePool<IO> pool;
    private CompletableTask<?> currentTask;
    private Handler handler;

    public IO setPool(final ResourcePool<IO> pool) {
        this.pool = pool;
        return this;
    }

    public ResourcePool<IO> getPool() {
        return pool;
    }
    
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
        this.writeBuffer = BufferOps.allocate(sessionInfo.getBufferSize(), sessionInfo.getDirectBuffers());
        this.readBuffer =  BufferOps.allocate(sessionInfo.getBufferSize(), sessionInfo.getDirectBuffers());
        this.sessionInfo = sessionInfo;
        this.feMessage = new FrontEndMessage(sessionInfo.getEncoding());
        feMessage.buffer = writeBuffer;
        this.channel = channel;
        oobHandlers.put(BackEnd.NoticeResponse, this::handleNotice);
        oobHandlers.put(BackEnd.ParameterStatus, this::handleParameterStatus);
        handler();
    }

    private void handler() {
        this.handler = new Handler();
        if(sessionInfo.getSsl()) {
            this.handler = new Handler();
            this.pool = ResourcePool.nullPool();
            CompletableTask<Boolean> sslTask = new SslTask().toCompletable();
            this.currentTask = sslTask;
            execute();
            try {
                final boolean val = sslTask.getFuture().get();
                if(!val) {
                    throw new UnsupportedOperationException("Ssl not supported by backend");
                }

                final SSLEngine engine = sessionInfo.getSslContext().createSSLEngine(sessionInfo.getHost(), sessionInfo.getPort());
                engine.setUseClientMode(true);
                try {
                    engine.beginHandshake();
                }
                catch(SSLException e) {
                    throw new RuntimeException(e);
                }
                final ByteBuffer sendBuffer = BufferOps.allocate(engine.getSession().getPacketBufferSize(), sessionInfo.getDirectBuffers());
                final ByteBuffer recvBuffer = BufferOps.allocate(engine.getSession().getPacketBufferSize(), sessionInfo.getDirectBuffers());

                this.handler = new HandshakeSslHandler(engine, sendBuffer, recvBuffer, Mode.WRITE);
            }
            catch(InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
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

    private enum Mode { READ, WRITE };
    
    private class Handler implements CompletionHandler<Integer,Mode> {
        
        public void failed(final Throwable ex, final Mode mode) {
            if(ex instanceof InterruptedByTimeoutException) {
                currentTask.onTimeout(feMessage, readBuffer);
                decide();
            }
            else {
                currentTask.onFail(ex);
                close();
                pool.bad(IO.this);
            }
        }

        public void writeComplete(final Integer bytes) {
            if(writeBuffer.hasRemaining()) {
                write();
            }
            else {
                writeBuffer.clear();
                prepareThread();
                currentTask.onWrite(feMessage, readBuffer);
                decide();
            }
        }

        public void readComplete(final Integer bytes) {
            readBuffer.flip();
            prepareThread();
            currentTask.onRead(feMessage, readBuffer);
            readBuffer.compact();
            decide();
        }

        public void write() {
            channel.write(writeBuffer, currentTask.getTimeout(), currentTask.getUnits(), Mode.WRITE, this);
        }

        public void read() {
            channel.read(readBuffer, currentTask.getTimeout(), currentTask.getUnits(), Mode.READ, this);
        }

        public void completed(final Integer bytes, final Mode mode) {
            if(mode == Mode.READ) {
                readComplete(bytes);
            }
            else {
                writeComplete(bytes);
            }
        }
    }

    private class SslHandler extends Handler {
        protected final SSLEngine engine;
        protected final ByteBuffer sendBuffer;
        protected final ByteBuffer recvBuffer;
        protected SSLEngineResult.HandshakeStatus hstatus;
        protected SSLEngineResult.Status status;

        public SslHandler(final SSLEngine engine, final ByteBuffer sendBuffer, final ByteBuffer recvBuffer) {
            this.engine = engine;
            this.sendBuffer = sendBuffer;
            this.recvBuffer = recvBuffer;
        }

        protected boolean isOK() {
            return status == SSLEngineResult.Status.OK;
        }

        protected boolean isOverflow() {
            return status == SSLEngineResult.Status.BUFFER_OVERFLOW;
        }

        protected boolean isUnderflow() {
            return status == SSLEngineResult.Status.BUFFER_UNDERFLOW;
        }

        protected boolean isClosed() {
            return status == SSLEngineResult.Status.CLOSED;
        }

        protected boolean isHandshaking() {
            return (hstatus != SSLEngineResult.HandshakeStatus.FINISHED &&
                    hstatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING);
        }

        protected boolean continueWrapping() {
            return (hstatus == SSLEngineResult.HandshakeStatus.NEED_WRAP ||
                    hstatus == SSLEngineResult.HandshakeStatus.NEED_TASK);
        }

        protected boolean continueUnwrapping() {
            return (hstatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP ||
                    hstatus == SSLEngineResult.HandshakeStatus.NEED_TASK);
        }

        protected boolean needWrap() {
            return hstatus == SSLEngineResult.HandshakeStatus.NEED_WRAP;
        }

        protected boolean needUnwrap() {
            return hstatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
        }

        protected boolean needTask() {
            return hstatus == SSLEngineResult.HandshakeStatus.NEED_TASK;
        }

        protected boolean needMoreData() {
            return status == SSLEngineResult.Status.BUFFER_UNDERFLOW;
        }

        protected boolean assign(final SSLEngineResult result) {
            this.hstatus = result.getHandshakeStatus();
            this.status = result.getStatus();
            return true;
        }

    }

    private class HandshakeSslHandler extends SslHandler {
        final ByteBuffer appSendBuffer = ByteBuffer.allocate(0);
        final ByteBuffer appRecvBuffer = BufferOps.allocate(engine.getSession().getApplicationBufferSize(), sessionInfo.getDirectBuffers());
        final Mode nextOp;
        
        public HandshakeSslHandler(final SSLEngine engine, final ByteBuffer sendBuffer, final ByteBuffer recvBuffer, final Mode nextOp) {
            super(engine, sendBuffer, recvBuffer);
            this.nextOp = nextOp;
            hstatus = engine.getHandshakeStatus();
        }

        @Override
        public void write() {
            handleWrap();
        }

        @Override
        public void read() {
            handleWrap();
        }

        public void handleWrap() {
            appSendBuffer.clear();
            sendBuffer.clear();

            while(continueWrapping()) {
                if(needTask()) {
                    tasks();
                }
                else if(needWrap()) {
                    try {
                        assign(engine.wrap(appSendBuffer, sendBuffer));
                    }
                    catch(SSLException e) {
                        failed(e, nextOp);
                        return;
                    }
                }
            }

            if(isHandshaking()) {
                sendBuffer.flip();
                channel.write(sendBuffer, 0L, TimeUnit.SECONDS, Mode.WRITE, this);
            }
            else {
                finish();
            }
        }

        public void handleUnwrap() {
            while(continueUnwrapping()) {
                if(needMoreData()) {
                    readMoreData();
                    return;
                }
                else if(needUnwrap()) {
                    try {
                        assign(engine.unwrap(recvBuffer, appRecvBuffer));
                    }
                    catch(SSLException e) {
                        failed(e, Mode.WRITE);
                        return;
                    }
                }
                else {
                    tasks();
                }
            }

            recvBuffer.compact();
            appRecvBuffer.clear();

            if(needWrap()) {
                handleWrap();
            }
            else {
                finish();
            }
        }

        private void readMoreData() {
            channel.read(recvBuffer, 0L, TimeUnit.SECONDS, Mode.READ, this);
        }

        @Override
        public void writeComplete(final Integer bytes) {
            if(sendBuffer.hasRemaining()) {
                channel.write(sendBuffer, 0L, TimeUnit.SECONDS, Mode.WRITE, this);
            }

            if(hstatus == SSLEngineResult.HandshakeStatus.NEED_UNWRAP) {
                readMoreData();
            }
            else {
                finish();
            }
        }

        @Override
        public void readComplete(final Integer bytes) {
            recvBuffer.flip();
            if(needWrap()) {
                handleWrap();
            }
            else if(needUnwrap()) {
                handleUnwrap();
            }
            else {
                finish();
            }
        }


        public void finish() {
            sendBuffer.clear();
            recvBuffer.clear();
            handler = toTraffic();
            if(nextOp == Mode.WRITE) {
                handler.write();
            }
            else {
                handler.read();
            }
        }

        private void tasks() {
            Runnable task;
            while((task = engine.getDelegatedTask()) != null) {
                task.run();
            }

            hstatus = engine.getHandshakeStatus();
        }

        public TrafficSslHandler toTraffic() {
             return new TrafficSslHandler(engine, sendBuffer, recvBuffer);
        }
    }

    private class TrafficSslHandler extends SslHandler {

        public TrafficSslHandler(final SSLEngine engine, final ByteBuffer sendBuffer, final ByteBuffer recvBuffer) {
            super(engine, sendBuffer, recvBuffer);
        }

        public HandshakeSslHandler toHandshake(final Mode mode) {
            return new HandshakeSslHandler(engine, sendBuffer, recvBuffer, mode);
        }

        @Override
        public void write() {
            try {
                while(writeBuffer.hasRemaining() && assign(engine.wrap(writeBuffer, sendBuffer)) && isOK()) { }
                
                if(isOK() || isOverflow()) {
                    sendBuffer.flip();
                    channel.write(sendBuffer, currentTask.getTimeout(), currentTask.getUnits(), Mode.WRITE, this);
                    return;
                }
                else if(isUnderflow()) {
                    failed(new BufferUnderflowException(), Mode.WRITE);
                    return;
                }
                else if(isClosed()) {
                    failed(new EOFException(), Mode.WRITE);
                    return;
                }
            }
            catch(SSLException e) {
                failed(e, Mode.WRITE);
            }
        }

        @Override
        public void writeComplete(final Integer bytes) {
            if(sendBuffer.hasRemaining()) {
                channel.write(sendBuffer, currentTask.getTimeout(), currentTask.getUnits(), Mode.WRITE, this);
            }
            else {
                sendBuffer.clear();
                super.writeComplete(bytes);
            }
        }

        @Override
        public void read() {
            channel.read(recvBuffer, currentTask.getTimeout(), currentTask.getUnits(), Mode.READ, this);
        }

        @Override
        public void readComplete(final Integer bytes) {
            recvBuffer.flip();

            try {
                while(recvBuffer.hasRemaining() && assign(engine.unwrap(recvBuffer, readBuffer)) && isOK()) { }

                recvBuffer.compact();
                if(isUnderflow()) {
                    read();
                    return;
                }
                else if(isClosed()) {
                    failed(new EOFException(), Mode.READ);
                }
            }
            catch(SSLException e) {
                failed(e, Mode.READ);
            }

            super.readComplete(bytes);
        }
    }
    
    private void prepareThread() {
        SerializationContext.io(this);
    }
    
    private void decide() {
        prepareThread();
        final TaskState state = currentTask.getNextState();
        if(state.next == TaskState.Next.START) {
            readBuffer.clear();
            writeBuffer.clear();
            currentTask.onStart(feMessage, readBuffer);
            decide();
        }
        else if(state.next == TaskState.Next.READ) {
            readBuffer = BufferOps.ensure(readBuffer, state.needs);
            handler.read();
        }
        else if(state.next == TaskState.Next.WRITE) {
            writeBuffer.flip();
            handler.write();
        }
        else if(state.next == TaskState.Next.FINISHED) {
            pool.good(this);
        }
        else if(state.next == TaskState.Next.TERMINATE) {
            close();
        }
        else {
            throw new UnsupportedOperationException(state.next + " is not a supported operation");
        }
    }
    
    public void execute(final CompletableTask<?> task) {
        if(pool == null) {
            throw new IllegalStateException("Pool is null");
        }
        
        if(!channel.isOpen()) {
            task.getFuture().completeExceptionally(new ClosedChannelException());
            pool.bad(this);
        }

        this.currentTask = task;
        execute();
    }

    private void execute() {
        readBuffer.clear();
        writeBuffer.clear();
        currentTask.setOobHandlers(oobHandlers);
        currentTask.setStatementCache(statementCache);
        prepareThread();
        currentTask.onStart(feMessage, readBuffer);
        decide();
    }
}
