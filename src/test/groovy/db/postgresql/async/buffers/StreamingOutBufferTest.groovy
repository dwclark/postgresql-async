package db.postgresql.async.buffers;

import spock.lang.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;

class StreamingOutBufferTest extends Specification {

    def "Test Numbers"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[128]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        out.beginMessage();
        int pos;
        
        when:
        out.put((byte) 1);
        then:
        bb.getInt(0) == bb.position();
        bb.get(bb.position() - 1) == 1;

        when:
        out.putShort((short) 2);
        then:
        bb.getInt(0) == bb.position();
        bb.getShort(bb.position() - 2) == 2;

        when:
        out.putInt(3);
        then:
        bb.getInt(0) == bb.position();
        bb.getInt(bb.position() - 4) == 3;

        when:
        out.putFloat(4.0f)
        then:
        bb.getInt(0) == bb.position();
        bb.getFloat(bb.position() - 4) == 4.0f;

        when:
        out.putLong(100L)
        then:
        bb.getInt(0) == bb.position();
        bb.getLong(bb.position() - 8) == 100L;

        when:
        out.putDouble(97.07d)
        then:
        bb.getInt(0) == bb.position();
        bb.getDouble(bb.position() - 8) == 97.07d;
    }

    def "Test Buffer Overflow"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[12]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        out.beginMessage();
        int pos;

        when:
        out.putDouble(17.7d);
        out.putDouble(19.9d);
        then:
        bb.getInt(0) == 20;
        bb.remaining() == 0;

        when:
        bb.clear();
        then:
        out.finished();
        bb.flip();
        bb.limit() == 8;
        bb.position() == 0;
        bb.getDouble() == 19.9d;
    }

    def "Test Multiple Records"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[100]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        out.beginMessage();
        out.put((byte) 0);
        out.beginMessage();
        out.put((byte) 1);

        expect:
        bb.position() == 10;
        bb.getInt(0) == 5;
        bb.get(4) == 0;
        bb.getInt(5) == 5;
        bb.get(9) == 1;
    }

    def "Test Write Ascii"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[100]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        out.beginMessage();
        out.putAsciiNullString('1234567890');

        expect:
        bb.position() == 15;
        bb.get(14) == 0;
        bb.get(12) == (byte) '9';
    }

    def "Test Byte Buffer"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[128]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        byte[] bytes = new byte[64];
        for(int i = 0; i < 64; ++i) {
            bytes[i] = (byte) i;
        }
        out.beginMessage();
        out.put(ByteBuffer.wrap(bytes));

        expect:
        bb.getInt(0) == 68;
        out.queue.peek() == null;
    }

    def "Test Byte Buffer Overflow"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[64]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        byte[] bytes = new byte[128];
        for(int i = 0; i < 128; ++i) {
            bytes[i] = (byte) i;
        }
        out.beginMessage();
        out.put(ByteBuffer.wrap(bytes));

        expect:
        bb.getInt(0) == 132;
        !bb.hasRemaining();
        out.queue.peek() != null;
        out.queue.peek().val.remaining() == 68;
    }

    def "Test Char Buffer"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[128]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        def cbuf = CharBuffer.wrap('1234567890');
        out.beginMessage();
        out.put(cbuf);

        expect:
        bb.getInt(0) == 14;
        out.queue.peek() == null;
        bb.get(4) == (byte) '1';
        bb.get(7) == (byte) '4';
    }

    def "Test Char Buffer Overflow"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[20]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        def cbuf = CharBuffer.wrap('abcdefghijklmnopqrstuvwxyz');
        out.beginMessage();
        out.put(cbuf);

        expect:
        bb.getInt(0) == 30;
        out.queue.peek() != null;
        out.queue.peek().val instanceof ByteBuffer;
        out.queue.peek().val.remaining() == 10;
    }

    def "Test Field Size"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[128]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        out.beginMessage();
        out.withFieldSize { o -> o.putInt(5); }

        expect:
        bb.getInt(0) == 12;
        bb.getInt(4) == 4;
        bb.getInt(8) == 5;
    }

    def "Test Multiple Field Sizes"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[128]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        out.beginMessage();
        out.withFieldSize { o -> o.put((byte) 5); };
        out.withFieldSize { o -> o.putShort((short) 5); };
        out.withFieldSize { o -> o.putLong(5L); };

        expect:
        bb.getInt(0) == 27
        bb.getInt(4) == 1;
        bb.get(8) == 5;
        bb.getInt(9) == 2;
        bb.getShort(13) == 5;
        bb.getInt(15) == 8;
        bb.getLong(19) == 5;
    }

    def "Test Complex Field"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[128]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        out.beginMessage();
        out.withFieldSize { o ->
            o.putLong(20L).putInt(10);
        }

        expect:
        bb.getInt(0) == 20;
        bb.getInt(4) == 12;
        bb.getLong(8) == 20L;
        bb.getInt(16) == 10;
    }

    def "Test Field Size With Overflow"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[24]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        def cbuf = CharBuffer.wrap('abcdefghijklmnopqrstuvwxyz');
        def bytes = new byte[100];
        for(int i = 0; i < 100; ++i) {
            bytes[i] = (byte) i;
        }
        def bbuf = ByteBuffer.wrap(bytes);
        out.beginMessage();
        out.withFieldSize { o -> o.put(cbuf); };
        out.withFieldSize { o -> o.put(bbuf); };

        expect:
        bb.getInt(0) == 138
        bb.getInt(4) == 26;
        !bb.hasRemaining();
        def first = out.queue.poll();
        first.val.remaining() == 10;
        def second = out.queue.poll();
        second.size == 100;
        def third = out.queue.poll();
        third.val.remaining() == 100;
        out.queue.poll() == null;
    }

    def "Test Finish/Fill"() {
        setup:
        def bb = ByteBuffer.wrap(new byte[10]);
        def out = new StreamingOutBuffer(bb, UTF_8.newEncoder());
        def cbuf = CharBuffer.wrap('abcdefghijklmnopqrstuvwxyz');
        def target = ByteBuffer.wrap(new byte[100]);
        out.beginMessage();
        out.put(cbuf);
        
        when:
        target.put(bb);
        then:
        !bb.hasRemaining();
        !out.finished();
        target.position(10);

        when:
        target.put(bb);
        then:
        !bb.hasRemaining();
        out.finished();
        target.position(20);

        when:
        target.put(bb);
        then:
        out.finished();
        target.position(30);
    }
}
