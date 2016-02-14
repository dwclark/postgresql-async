package db.postgresql.async;

public interface ResourcePool<T> {
    T fast();
    T guaranteed();
    void good(T o);
    void bad(T o);
    void shutdown();

    public static class NullPool<T> implements ResourcePool<T> {
        public T fast() { return null; }
        public T guaranteed() { return null; };
        public void good(T o) { }
        public void bad(T o) { }
        public void shutdown() { }
    }

    public static <T> ResourcePool<T> nullPool() {
        return new NullPool<>();
    }
}
