package db.postgresql.async;

public interface <T> ResourcePool {
    T fast();
    T guaranteed();

    void good(T o);
    void bad(T o);
}
