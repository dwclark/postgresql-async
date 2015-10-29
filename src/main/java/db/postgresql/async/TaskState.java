package db.postgresql.async;

import java.nio.ByteBuffer;

class TaskState {

    private static final int FINISHED = 0b0001;
    private static final int MORE = 0b0010;
    private static final int AT_LEAST = 0b0100;
    private static final int WRITE = 0b1000;
    
    private final int need;
    private final Object payload;

    private TaskState(final int need, final Object payload) {
        this.need = need;
        this.payload = payload;
    }
    
    public static TaskState finished() {
        return new TaskState(FINISHED, null);
    }

    public static TaskState more() {
        return new TaskState(MORE, null);
    }
    
    public static TaskState atLeast(final Integer bytes) {
        return new TaskState(MORE | AT_LEAST, bytes);
    }

    public static TaskState write(final ByteBuffer buffer) {
        return new TaskState(WRITE, buffer);
    }

    public boolean isFinished() {
        return (FINISHED & need) != 0;
    }

    public boolean isMore() {
        return (MORE & need) != 0;
    }

    public boolean isAtLeast() {
        return (AT_LEAST & need) != 0;
    }

    public boolean isWrite() {
        return (WRITE & need) != 0;
    }

    public int getBytes() {
        if(!isAtLeast()) {
            throw new IllegalStateException("No bytes specified");
        }

        return (Integer) payload;
    }

    public ByteBuffer getByteBuffer() {
        if(!isWrite()) {
            throw new IllegalStateException("No byte buffer specified");
        }

        return (ByteBuffer) payload;
    }
}
