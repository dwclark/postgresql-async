package db.postgresql.async;

import db.postgresql.async.buffers.BufferOps;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.util.concurrent.TimeUnit;
import java.nio.ByteBuffer;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.Response;
import db.postgresql.async.pginfo.StatementCache;
import java.util.Map;
import java.util.function.Consumer;
import java.util.concurrent.ExecutionException;

public class AsynchronousSslChannel extends AsynchronousSocketChannel {

    private final AsynchronousSocketChannel delegate;
    private final SSLEngine engine;
    private final ByteBuffer sendBuffer;
    private final ByteBuffer recvBuffer;

    public AsynchronousSslChannel(final AsynchronousSocketChannel delegate,
                                  final SessionInfo info, final SSLContext sslContext) {
        super(delegate.provider());
        this.delegate = delegate;
        this.engine = sslContext.createSSLEngine(info.getHost(), info.getPort());
        engine.setUseClientMode(true);
        engine.setWantClientAuth(false);
        final int minSize = engine.getSession().getPacketBufferSize();
        this.sendBuffer = BufferOps.allocate(Math.max(minSize, info.getBufferSize()), info.getDirectBuffers());
        this.recvBuffer = BufferOps.allocate(Math.max(minSize, info.getBufferSize()), info.getDirectBuffers());
    }

    public AsynchronousSocketChannel bind(final SocketAddress socketAddress) throws IOException {
        delegate.bind(socketAddress);
        return this;
    }

    public <T> AsynchronousSocketChannel setOption(final SocketOption<T> socketOption, final T t) throws IOException {
        delegate.setOption(socketOption, t);
        return this;
    }

    public <T> T getOption(final SocketOption<T> socketOption) throws IOException {
        return delegate.getOption(socketOption);
    }

    public Set<SocketOption<?>> supportedOptions() {
        return delegate.supportedOptions();
    }

    public AsynchronousSocketChannel shutdownInput() throws IOException {
        delegate.shutdownInput();
        return this;
    }

    public AsynchronousSocketChannel shutdownOutput() throws IOException {
        delegate.shutdownOutput();
        return this;
    }

    public SocketAddress getRemoteAddress() throws IOException {
        return delegate.getRemoteAddress();
    }

    public <A> void connect(final SocketAddress socketAddress, final A a, final CompletionHandler<Void, ? super A> completionHandler) {
        delegate.connect(socketAddress, a, completionHandler);
    }

    public Future<Void> connect(final SocketAddress socketAddress) {
        return delegate.connect(socketAddress);
    }

    public <A> void read(final ByteBuffer byteBuffer, final long l, final TimeUnit timeUnit,
                         final A a, final CompletionHandler<Integer, ? super A> completionHandler) {
        delegate.read(byteBuffer, l, timeUnit, a, completionHandler);
    }

    public Future<Integer> read(final ByteBuffer byteBuffer) {
        return delegate.read(byteBuffer);
    }

    public <A> void read(final ByteBuffer[] byteBuffers, final int i, final int i1,
                         final long l, final TimeUnit timeUnit, final A a, final CompletionHandler<Long, ? super A> completionHandler) {
        throw new UnsupportedOperationException();
    }

    public <A> void write(final ByteBuffer byteBuffer, final long l, final TimeUnit timeUnit,
                          final A a, final CompletionHandler<Integer, ? super A> completionHandler) {
        delegate.write(byteBuffer, l, timeUnit, a, completionHandler);
    }

    public Future<Integer> write(final ByteBuffer byteBuffer) {
        return delegate.write(byteBuffer);
    }

    public <A> void write(ByteBuffer[] byteBuffers, int i, int i1, long l, TimeUnit timeUnit, A a, CompletionHandler<Long, ? super A> completionHandler) {
        throw new UnsupportedOperationException();
    }

    public SocketAddress getLocalAddress() throws IOException {
        return delegate.getLocalAddress();
    }

    public boolean isOpen() {
        return delegate.isOpen();
    }

    public void close() throws IOException {
        delegate.close();
    }

    public void startup(final IO io) {
        try {
            final CompletableTask<Boolean> ct = new StartupTask().toCompletable();
            io.setPool(new NullPool()).execute(ct);
            final boolean result = ct.getFuture().get();
            if(!result) {
                throw new UnsupportedOperationException("SSL not supported");
            }
        }
        catch(InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static class NullPool implements ResourcePool<IO> {
        public IO fast() { return null; }
        public IO guaranteed() { return null; }
        public void good(IO o) { }
        public void bad(IO o) { }
        public void shutdown() { }
    }

    private abstract class BaseSslHandler<T> implements Task<T> {
        protected TaskState nextState = TaskState.start();
        protected boolean executed = false;
        protected TransactionStatus status;
        protected Throwable error;
        
        public TransactionStatus getTransactionStatus() { return status; }
        public CommandStatus getCommandStatus() { return null; }
        public Throwable getError() { return error; }
        public void setError(Throwable t) { error = t; }

        public void onFail(Throwable t) {
            nextState = TaskState.terminate();
        }
            
        public void onTimeout(FrontEndMessage fe, ByteBuffer readBuffer) {
            nextState = TaskState.terminate();
        }
        
        public TaskState getNextState() { return nextState; }
        
        public long getTimeout() { return 0L; }
        
        public TimeUnit getUnits() { return TimeUnit.SECONDS; }

        public void setOobHandlers(final Map<BackEnd,Consumer<Response>> oobHandlers) { }
        
        public void setStatementCache(StatementCache cache) { }
        
        public void executed() { executed = true; }
        
        public boolean isExecuted() { return executed; }
    }

    public static final byte CONTINUE = (byte) 'S';
    public static final byte STOP = (byte) 'N';
    
    private class StartupTask extends BaseSslHandler<Boolean> {
        private Boolean result;

        public Boolean getResult() { return result; }
        
        public void onStart(FrontEndMessage fe, ByteBuffer readBuffer) {
            fe.ssl();
            nextState = TaskState.write();
        }
        
        public void onRead(FrontEndMessage fe, ByteBuffer readBuffer) {
            if(readBuffer.hasRemaining()) {
                final byte resp = readBuffer.get();
                if(resp == CONTINUE) {
                    result = Boolean.TRUE;
                    status = TransactionStatus.IDLE;
                }
                else {
                    result = Boolean.FALSE;
                    status = TransactionStatus.FAILED;
                }

                nextState = TaskState.finished();
            }
            else {
                nextState = TaskState.read();
            }
        }
        
        public void onWrite(FrontEndMessage fe, ByteBuffer readBuffer) {
            if(fe.buffer.hasRemaining()) {
                nextState = TaskState.write();
            }
            else {
                nextState = TaskState.read();
            }
        }
    }

    private class Ssl extends BaseSslHandler<Void> {

        public Void getResult() { return null; }

        public Ssl() {
            engine.beginHandshake();
        }
        
        public void onStart(FrontEndMessage fe, ByteBuffer readBuffer) {

        }
        
        public void onRead(FrontEndMessage fe, ByteBuffer readBuffer) {

        }
        
        public void onWrite(FrontEndMessage fe, ByteBuffer readBuffer) {

        }
    }
}
