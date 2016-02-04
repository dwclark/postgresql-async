package db.postgresql.async;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AsynchronousSslChannel extends AsynchronousSocketChannel {

    private final AsynchronousSocketChannel delegate;

    public AsynchronousSslChannel(final AsynchronousSocketChannel delegate) {
        super(delegate.provider());
        this.delegate = delegate;
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
}
