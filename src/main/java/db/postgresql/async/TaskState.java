package db.postgresql.async;

public class TaskState {

    public enum Next { READ, WRITE, FINISHED, TERMINATE };
    
    public final int needs;
    public final Next next;
    
    private static final TaskState READ = new TaskState(Next.READ, 0);
    private static final TaskState WRITE = new TaskState(Next.WRITE, 0);
    private static final TaskState FINISHED = new TaskState(Next.FINISHED, -1);
    private static final TaskState TERMINATE = new TaskState(Next.TERMINATE, -1);

    private TaskState(final Next next, final int needs) {
        this.next = next;
        this.needs = needs;
    }
    
    public static TaskState read() {
        return READ;
    }

    public static TaskState needs(final int bytes) {
        return new TaskState(Next.READ, bytes);
    }

    public static TaskState write() {
        return WRITE;
    }

    public static TaskState finished() {
        return FINISHED;
    }

    public static TaskState terminate() {
        return TERMINATE;
    }
}
