package db.postgresql.async.buffers;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.nio.BufferOverflowException;

public class BufferOps {

    public static final int DEFAULT_SIZE = 32_768;
    private static final int[] ALLOWED = new int[] { 4096, 8192, 16_384, 32_768, 65_536,
                                                     131_072, 262_144, 524_288,
                                                     1_048_576, 2_097_152, 4_194_304,
                                                     8_388_608, 16_777_216, 33_554_432,
                                                     67_108_864, 134_217_728, 268_435_456,
                                                     536_870_912, 1_073_741_824, 2_147_483_647 };
    
    public static ByteBuffer allocate(final int size, final boolean direct) {
        return (direct ?
                ByteBuffer.allocateDirect(nextAllocation(size)) :
                ByteBuffer.allocate(nextAllocation(size)));
    }

    public static ByteBuffer allocate(final boolean direct) {
            return allocate(DEFAULT_SIZE, direct);
    }

    public static ByteBuffer sizeUp(final ByteBuffer src) {
        final ByteBuffer ret = allocate(src.capacity() + 1, src.isDirect());
        final int position = src.position();
        src.flip();
        ret.put(src);
        ret.limit(ret.capacity());
        ret.position(position);
        return ret;
    }

    public static ByteBuffer ensure(final ByteBuffer src, final int size) {
        if(src.capacity() - src.position() < size) {
            return sizeUp(src);
        }
        else {
            return src;
        }
    }    

    public static final int nextAllocation(final int size) {
        for(int i = 0; i < ALLOWED.length; ++i) {
            if(size <= ALLOWED[i]) {
                return ALLOWED[i];
            }
        }

        throw new IllegalStateException("You should never get here!");
    }

    public static ByteBuffer putWithSize(final ByteBuffer buffer, final Consumer<ByteBuffer> consumer) {
        final int start = buffer.position();
        try {
            buffer.putInt(0);
            final int startData = buffer.position();
            consumer.accept(buffer);
            final int endData = buffer.position();
            buffer.putInt(start, endData - startData);
            return buffer;
        }
        catch(BufferOverflowException ex) {
            buffer.position(start);
            return putWithSize(sizeUp(buffer), consumer);
        }
    }

    public static ByteBuffer putNull(final ByteBuffer buffer) {
        final int start = buffer.position();
        try {
            return buffer.putInt(-1);
        }
        catch(BufferOverflowException ex) {
            buffer.position(start);
            return putNull(buffer);
        }
    }
}
