package db.postgresql.async.buffers;

import spock.lang.*;
import java.nio.ByteBuffer;

class BufferOpsTest extends Specification {

    def "Test Allocation"() {
        setup:
        ByteBuffer buffer = BufferOps.allocate(1, false);

        expect:
        buffer.capacity() == BufferOps.ALLOWED[0]
    }

    def "Test Re Allocation"() {
        setup:
        ByteBuffer buffer = BufferOps.allocate(2, true);
        buffer = BufferOps.sizeUp(buffer, 1);

        expect:
        buffer.capacity() == BufferOps.ALLOWED[1];
        buffer.direct;
    }

    def "Test Size Up"() {
        setup:
        ByteBuffer buffer = BufferOps.allocate(1, false);
        (0..99).each { buffer.putLong(100L); }
        buffer = BufferOps.ensure(buffer, 4096);

        expect:
        buffer.capacity() == BufferOps.ALLOWED[1];
        !buffer.direct;
    }
}
