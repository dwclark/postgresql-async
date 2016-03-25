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
import java.util.function.Consumer;

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
    
    private abstract class SizeHeader {
        private SizeHeader auxiliary;

        public void setAuxiliary(final SizeHeader val) {
            this.auxiliary = val;
        }

        public void removeAuxiliary() {
            auxiliary = null;
        }

        public void increment(int amt) {
            if(auxiliary != null) {
                auxiliary.increment(amt);
            }
        }
        
        abstract int size();
    }
    
    private class SizeHeaderInBuffer extends SizeHeader {
        int position;

        public SizeHeaderInBuffer(final int initial) {
            this.position = buffer.position();
            buffer.putInt(initial);
        }

        public void increment(final int amt) {
            super.increment(amt);
            buffer.putInt(position, buffer.getInt(position) + amt);
        }

        public int size() {
            return buffer.getInt(position);
        }

        public void move(final int position, final int initial) {
            this.position = position;
            buffer.putInt(initial);
        }
    }

    private class SizeHeaderInMemory extends SizeHeader implements Predicate<ByteBuffer> {
        int size;

        public SizeHeaderInMemory(final int initial) {
            this.size = initial;
        }

        public void increment(final int amt) {
            super.increment(amt);
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

    private SizeHeader fieldSizeHeader;
    private SizeHeader sizeHeader;
    private State state = State.BUFFER;
    
    public StreamingOutBuffer(final ByteBuffer buffer, final CharsetEncoder encoder) {
        this.buffer = buffer;
        this.encoder = encoder;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public Queue<Predicate<ByteBuffer>> getQueue() {
        return queue;
    }

    public CharsetEncoder getEncoder() {
        return encoder;
    }

    public void beginMessage() {
        final int initial = 4;
        
        if(buffer.remaining() >= 4 && state == State.BUFFER) {
            if(sizeHeader == null) {
                sizeHeader = new SizeHeaderInBuffer(initial);
            }
            else {
                ((SizeHeaderInBuffer) sizeHeader).move(buffer.position(), initial);
            }
        }
        else {
            state = State.QUEUE;
            SizeHeaderInMemory shim = new SizeHeaderInMemory(initial);
            queue.add(shim);
            sizeHeader = shim;
        }
    }

    private void beginField() {
        final int initial = 0;
        sizeHeader.increment(4);
        
        if(buffer.remaining() >= 4 && state == State.BUFFER) {
            if(fieldSizeHeader == null) {
                fieldSizeHeader = new SizeHeaderInBuffer(initial);
            }
            else {
                ((SizeHeaderInBuffer) fieldSizeHeader).move(buffer.position(), initial);
            }
        }
        else {
            state = State.QUEUE;
            SizeHeaderInMemory shim = new SizeHeaderInMemory(initial);
            queue.add(shim);
            fieldSizeHeader = shim;
        }

        sizeHeader.setAuxiliary(fieldSizeHeader);
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
        return queue.peek() == null;
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
    
    public StreamingOutBuffer withFieldSize(final Consumer<OutBuffer> consumer) {
        try {
            beginField();
            consumer.accept(this);
        }
        finally {
            sizeHeader.removeAuxiliary();
        }

        return this;
    }
}
