package db.postgresql.async.serializers;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.BufferOverflowException;

public class SerializationContext {

    public static class StringOps {
        
        private CharBuffer charBuffer = CharBuffer.allocate(1024);
        private Charset encoding = Charset.forName("UTF-8");
        private CharsetEncoder encoder = encoding.newEncoder();
        private CharsetDecoder decoder = encoding.newDecoder();
        
        public CharBuffer ensure(final int size) {
            if(charBuffer.capacity() < size) {
                charBuffer = CharBuffer.allocate(1024);
            }

            charBuffer.clear();
            return charBuffer;
        }

        public void setEncoding(final Charset val) {
            this.encoding = val;
            this.encoder = val.newEncoder();
            this.decoder = val.newDecoder();
        }

        public Charset getEncoding() {
            return encoding;
        }

        public CharsetEncoder getEncoder() {
            return encoder.reset();
        }

        public CharsetDecoder getDecoder() {
            return decoder.reset();
        }
    }
    
    private static final ThreadLocal<StringOps> stringOps = new ThreadLocal<StringOps>() {
            @Override protected StringOps initialValue() {
                return new StringOps();
            } };

    public static StringOps stringOps() { return stringOps.get(); }

    public static void encoding(final Charset charset) {
        stringOps.get().setEncoding(charset);
    }

    public static String bufferToString(final ByteBuffer buffer, final int size) {
        final StringOps ops = stringOps.get();
        final CharBuffer targetBuffer = ops.ensure(size);
        final ByteBuffer sourceBuffer = (ByteBuffer) buffer.slice().limit(size);
        final CharsetDecoder decoder = ops.getDecoder();
        decoder.decode(sourceBuffer, targetBuffer, false);
        decoder.decode(sourceBuffer, targetBuffer, true);
        decoder.flush(targetBuffer);
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
