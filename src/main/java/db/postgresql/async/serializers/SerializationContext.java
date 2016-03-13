package db.postgresql.async.serializers;

import db.postgresql.async.IO;
import db.postgresql.async.messages.RowDescription;
import db.postgresql.async.pginfo.Registry;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.BufferOverflowException;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SerializationContext {

    public static class StringOps {
        private static final int MIN = 1024;
        private static final int MAX = 1_073_741_824;
        private CharBuffer charBuffer = CharBuffer.allocate(MIN);
        private Charset encoding = UTF_8;
        private CharsetEncoder encoder = null;
        private CharsetDecoder decoder = null;

        private int nextAllocation(final int size) {
            int accum = MIN * size;
            while(accum < size && accum != MAX) {
                accum *= 2;
            }

            return accum;
        }
        
        public CharBuffer ensure(final int size) {
            if(charBuffer.capacity() < size) {
                charBuffer = CharBuffer.allocate(nextAllocation(size));
            }

            charBuffer.clear();
            return charBuffer;
        }

        public void setEncoding(final Charset val) {
            this.encoding = val;
            this.encoder = null;
            this.decoder = null;
        }

        public Charset getEncoding() {
            return encoding;
        }

        public CharsetEncoder getEncoder() {
            if(encoder == null) {
                encoder = encoding.newEncoder();
            }
            
            return encoder.reset();
        }

        public CharsetDecoder getDecoder() {
            if(decoder == null) {
                decoder = encoding.newDecoder();
            }
            
            return decoder.reset();
        }
    }
    
    private static final ThreadLocal<StringOps> stringOps = new ThreadLocal<StringOps>() {
            @Override protected StringOps initialValue() {
                return new StringOps();
            } };

    private static final ThreadLocal<RowDescription> description = new ThreadLocal<RowDescription>();

    private static final ThreadLocal<Registry> registry = new ThreadLocal<Registry>();

    public static void description(final RowDescription val) {
        description.set(val);
    }

    public static RowDescription description() {
        return description.get();
    }
    
    public static Registry registry() {
        return registry.get();
    }

    public static void registry(final Registry val) {
        registry.set(val);
    }

    private static final ThreadLocal<IO> currentIO = new ThreadLocal<IO>();

    public static void io(final IO io) {
        currentIO.set(io);
        registry.set(io.getSessionInfo().getRegistry());
        stringOps().setEncoding(io.getSessionInfo().getEncoding());
    }

    public static IO io() {
        return currentIO.get();
    }
    
    public static StringOps stringOps() { return stringOps.get(); }

    public static String bufferToString(final ByteBuffer buffer) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        else {
            return bufferToString(size, buffer);
        }
    }

    public static String bufferToString(final int size, final ByteBuffer buffer) {
        final StringOps ops = stringOps.get();
        final CharBuffer targetBuffer = ops.ensure(size);
        final int limit = buffer.limit();
        buffer.limit(buffer.position() + size);
        final CharsetDecoder decoder = ops.getDecoder();
        decoder.decode(buffer, targetBuffer, false);
        decoder.decode(buffer, targetBuffer, true);
        decoder.flush(targetBuffer);
        buffer.limit(limit);
        targetBuffer.flip();
        return targetBuffer.toString();
    }

    public static void stringToBuffer(final ByteBuffer buffer, final CharSequence seq) {
        final StringOps ops = stringOps.get();
        final CharBuffer sourceBuffer = CharBuffer.wrap(seq);
        final CharsetEncoder encoder = ops.getEncoder();
        final CoderResult result = encoder.encode(sourceBuffer, buffer, false);
        if(result == CoderResult.OVERFLOW) {
            throw new BufferOverflowException();
        }
        encoder.encode(sourceBuffer, buffer, true);
        encoder.flush(buffer);
    }
}
