package db.postgresql.async;

public interface ResourcePool<T> {
    T fast();
    T guaranteed();
    void good(T o);
    void bad(T o);
}
