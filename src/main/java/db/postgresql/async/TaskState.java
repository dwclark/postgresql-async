package db.postgresql.async;

public class TaskState {

    enum Next { FINISHED, MORE, AT_LEAST };

    public final Next next;
    public final int bytes;

    private TaskState(final Next next, final int bytes) {
        this.next = next;
        this.bytes = bytes;
    }
    
    public static TaskState finished() {
        return new TaskState(Next.FINISHED, 0);
    }

    public static TaskState more() {
        return new TaskState(Next.MORE, 0);
    }

    public static TaskState atLeast(int bytes) {
        return new TaskState(Next.AT_LEAST, bytes);
    }
}
