package db.postgresql.async;

public interface TaskIterator<T> {
    T getAccumulator();
    boolean hasNext(Task<?> previous);
    Task<?> next();
}
