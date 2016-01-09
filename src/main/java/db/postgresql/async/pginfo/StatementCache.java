package db.postgresql.async.pginfo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StatementCache {
    private final ConcurrentMap<String,Statement> cache = new ConcurrentHashMap<>(20, 0.75f, 1);

    public void store(final String query, final Statement statement) {
        cache.put(query, statement);
    }

    public Statement statement(final String query) {
        return (Statement) cache.get(query);
    }

    public int size() {
        return cache.size();
    }
}
