package db.postgresql.async.pginfo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StatementCache {
    private final AtomicInteger counter = new AtomicInteger(0);
    private final ConcurrentMap<String,String> cache = new ConcurrentHashMap<>(20, 0.75f, 1);

    public String name(final String query) {
        return cache.get(query);
    }

    public boolean parsed(final String query) {
        return cache.containsKey(query);
    }

    public String add(final String query) {
        final String interned = query.intern();
        final String value = "_" + Integer.toString(counter.getAndIncrement());
        cache.put(query, value);
        return value;
    }
}
