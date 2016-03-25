package db.postgresql.async.buffers;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import static db.postgresql.async.buffers.BufferOps.*;
import static java.nio.charset.StandardCharsets.US_ASCII;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;
import java.util.function.Predicate;

public class StreamingOutBuffer implements OutBuffer {

    //private classses for buffer management
    private static class BytePredicate implements Predicate<ByteBuffer> {
        private final byte val;

        public BytePredicate(final byte val) {
            this.val = val;
        }
        
        public boolean test(final ByteBuffer buffer) {
            if(buffer.remaining() == 0) {
                return false;
            }
            else {
                buffer.put(val);
                return true;
            }
        }
    }

    private static class ShortPredicate implements Predicate<ByteBuffer> {
        private final short val;

        public ShortPredicate(final short val) {
            this.val = val;
        }
        
        public boolean test(final ByteBuffer buffer) {
            if(buffer.remaining() < 2) {
                return false;
            }
            else {
                buffer.putShort(val);
                return true;
            }
        }
    }

    private static class IntPredicate implements Predicate<ByteBuffer> {
        private final int val;

        public IntPredicate(final int val) {
            this.val = val;
        }
        
        public boolean test(final ByteBuffer buffer) {
            if(buffer.remaining() < 4) {
                return false;
            }
            else {
                buffer.putInt(val);
                return true;
            }
        }
    }

    private static class LongPredicate implements Predicate<ByteBuffer> {
        private final long val;

        public LongPredicate(final long val) {
            this.val = val;
        }
        
        public boolean test(final ByteBuffer buffer) {
            if(buffer.remaining() < 4) {
                return false;
            }
            else {
                buffer.putLong(val);
                return true;
            }
        }
    }

    private static class FloatPredicate implements Predicate<ByteBuffer> {
        private final float val;

        public FloatPredicate(final float val) {
            this.val = val;
        }
        
        public boolean test(final ByteBuffer buffer) {
            if(buffer.remaining() < 4) {
                return false;
            }
            else {
                buffer.putFloat(val);
                return true;
            }
        }
    }

    private static class DoublePredicate implements Predicate<ByteBuffer> {
        private final double val;

        public DoublePredicate(final double val) {
            this.val = val;
        }
        
        public boolean test(final ByteBuffer buffer) {
            if(buffer.remaining() < 4) {
                return false;
            }
            else {
                buffer.putDouble(val);
                return true;
            }
        }
    }

    private static void writePossible(final ByteBuffer target, final ByteBuffer src) {
        final int size = Math.min(target.remaining(), src.remaining());
        final int originalLimit = src.limit();
        src.limit(src.position() + size);
        target.put(src);
        src.limit(originalLimit);
    }
    
    private static class ByteBufferPredicate implements Predicate<ByteBuffer> {
        private final ByteBuffer val;
        
        public ByteBufferPredicate(final ByteBuffer val) {
            this.val = val;
        }

        public boolean test(final ByteBuffer target) {
            writePossible(target, val);
            return !val.hasRemaining();
        }
    }

    //private classes for state management
    private enum State { BUFFER, QUEUE };
    
    private interface SizeHeader {
        void increment(int amt);
        int size();
    }
    
    private class SizeHeaderInBuffer implements SizeHeader {
        final int position;

        public SizeHeaderInBuffer() {
            this.position = buffer.position();
            buffer.putInt(4);
        }

        public void increment(final int amt) {
            buffer.putInt(position, buffer.getInt(position) + amt);
        }

        public int size() {
            return buffer.getInt(position);
        }
    }

    private class SizeHeaderInMemory implements SizeHeader, Predicate<ByteBuffer> {
        int size;

        public SizeHeaderInMemory() {
            this.size = 4;
        }

        public void increment(final int amt) {
            size += amt;
        }

        public int size() {
            return size;
        }

        public boolean test(final ByteBuffer buffer) {
            if(buffer.remaining() < 4) {
                return false;
            }
            else {
                buffer.putInt(size);
                return true;
            }
        }
    }

    private final ByteBuffer buffer;
    private final Queue<Predicate<ByteBuffer>> queue = new ConcurrentLinkedQueue<>();
    private final CharsetEncoder encoder;
    private final CharsetEncoder asciiEncoder = US_ASCII.newEncoder();
    
    private SizeHeader sizeHeader;
    private State state;
    
    public StreamingOutBuffer(final ByteBuffer buffer, final CharsetEncoder encoder) {
        this.buffer = buffer;
        this.state = State.BUFFER;
        this.sizeHeader = new SizeHeaderInBuffer();
        this.encoder = encoder;
    }

    public void beginMessage() {
        if(buffer.remaining() >= 4 && state == State.BUFFER) {
            sizeHeader = new SizeHeaderInBuffer();
        }
        else {
            state = State.QUEUE;
            SizeHeaderInMemory shim = new SizeHeaderInMemory();
            queue.add(shim);
            sizeHeader = shim;
        }
    }
    
    public void clear() {
        buffer.clear();
        queue.clear();
        state = State.BUFFER;
    }

    public void fill() {
        while(queue.peek() != null) {
            if(queue.peek().test(buffer)) {
                queue.poll();
            }
            else {
                break;
            }
        }
    }

    public boolean finished() {
        buffer.clear();
        fill();
        return (buffer.remaining() == 0) && (queue.peek() == null);
    }

    public StreamingOutBuffer put(final byte val) {
        sizeHeader.increment(1);
        if(state == State.BUFFER && buffer.remaining() >= 1) {
            buffer.put(val);
        }
        else {
            state = State.QUEUE;
            queue.add(new BytePredicate(val));
        }

        return this;
    }
    
    public StreamingOutBuffer putShort(final short val) {
        sizeHeader.increment(2);
        if(state == State.BUFFER && buffer.remaining() >= 2) {
            buffer.putShort(val);
        }
        else {
            state = State.QUEUE;
            queue.add(new ShortPredicate(val));
        }

        return this;
    }
    
    public StreamingOutBuffer putInt(final int val) {
        sizeHeader.increment(4);
        if(state == State.BUFFER && buffer.remaining() >= 4) {
            buffer.putInt(val);
        }
        else {
            state = State.QUEUE;
            queue.add(new IntPredicate(val));
        }

        return this;
    }
    
    public StreamingOutBuffer putLong(final long val) {
        sizeHeader.increment(8);
        if(state == State.BUFFER && buffer.remaining() >= 8) {
            buffer.putLong(val);
        }
        else {
            state = State.QUEUE;
            queue.add(new LongPredicate(val));
        }

        return this;
    }
    
    public StreamingOutBuffer putFloat(final float val) {
        sizeHeader.increment(4);
        if(state == State.BUFFER && buffer.remaining() >= 4) {
            buffer.putFloat(val);
        }
        else {
            state = State.QUEUE;
            queue.add(new FloatPredicate(val));
        }

        return this;
    }
        
    public StreamingOutBuffer putDouble(final double val) {
        sizeHeader.increment(8);
        if(state == State.BUFFER && buffer.remaining() >= 8) {
            buffer.putDouble(val);
        }
        else {
            state = State.QUEUE;
            queue.add(new DoublePredicate(val));
        }

        return this;
    }

    private void queueCharBuffer(final CharBuffer cbuf, final CharsetEncoder enc) {
        state = State.QUEUE;
        final int maxSize = (int) Math.ceil(enc.maxBytesPerChar() * cbuf.remaining());
        final ByteBuffer bb = ByteBuffer.wrap(new byte[maxSize]);
        enc.encode(cbuf, bb, true);
        enc.flush(bb);
        bb.flip();
        put(bb);
    }

    private CoderResult toMainBuffer(final CharBuffer cbuf, final CharsetEncoder enc) {
        if(state == State.BUFFER) {
            final int starting = buffer.position();
            final CoderResult result = enc.encode(cbuf, buffer, true);
            if(result == CoderResult.OVERFLOW || enc.flush(buffer) == CoderResult.OVERFLOW) {
                sizeHeader.increment(buffer.position() - starting);
                return CoderResult.OVERFLOW;
            }
            else {
                sizeHeader.increment(buffer.position() - starting);
                return CoderResult.UNDERFLOW;
            }
        }
        else {
            return CoderResult.OVERFLOW;
        }
    }

    public StreamingOutBuffer putAsciiNullString(final String val) {
        asciiEncoder.reset();
        final CharBuffer cbuf = CharBuffer.wrap(val);
        final CoderResult result = toMainBuffer(cbuf, asciiEncoder);
        if(result == CoderResult.OVERFLOW) {
            queueCharBuffer(cbuf, asciiEncoder);
        }

        return put((byte) 0);
    }

    public StreamingOutBuffer put(final String val) {
        return put(CharBuffer.wrap(val));
    }

    public StreamingOutBuffer put(final byte[] val) {
        return put(ByteBuffer.wrap(val));
    }
    
    public StreamingOutBuffer put(final ByteBuffer val) {
        sizeHeader.increment(val.remaining());
        writePossible(buffer, val);
        if(val.hasRemaining()) {
            queue.add(new ByteBufferPredicate(val));
        }

        return this;
    }

    public StreamingOutBuffer put(final CharBuffer val) {
        encoder.reset();
        final CoderResult result = toMainBuffer(val, encoder);
        if(result == CoderResult.OVERFLOW) {
            queueCharBuffer(val, encoder);
        }

        return this;
    }
}
