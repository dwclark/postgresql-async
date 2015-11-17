package db.postgresql.async.pginfo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PgSessionCache {
    private final ConcurrentMap<Key,Object> cache = new ConcurrentHashMap<>(20, 0.75f, 1);

    private abstract static class Key {
        public abstract Class getType();
        public abstract String getId();
        
        @Override
        public int hashCode() {
            return 1303 * (3637 + getType().hashCode()) + getId().hashCode();
        }

        @Override
        public boolean equals(final Object rhs) {
            if(!(rhs instanceof Key)) {
                return false;
            }

            Key o = (Key) rhs;
            return (getType() == o.getType() &&
                    getId().equals(o.getId()));
        }
    }

    public static class ImmutableKey extends Key {
        private final Class type;
        public Class getType() { return type; }

        private final String id;
        public String getId() { return id; }

        public ImmutableKey(final Class type, final String id) {
            this.type = type;
            this.id = id;
        }
    }

    private static class MutableKey extends Key {
        private Class type;
        public Class getType() { return type; }

        private String id;
        public String getId() { return id; }

        private static final ThreadLocal<MutableKey> tl = new ThreadLocal<MutableKey>() {
                @Override public MutableKey initialValue() {
                    return new MutableKey();
                }
            };

        public static MutableKey make(final Class type, final String id) {
            MutableKey key = tl.get();
            key.type = type;
            key.id = id;
            return key;
        }
    }

    public void store(final String query, final Statement statement) {
        Key key = new ImmutableKey(Statement.class, query);
        cache.put(key, statement);
    }

    public Statement statement(final String query) {
        return (Statement) cache.get(MutableKey.make(Statement.class, query));
    }

    public void store(final String id, final Portal portal) {
        Key key = new ImmutableKey(Portal.class, id);
        cache.put(key, portal);
    }

    public Portal portal(final String id) {
        return (Portal) cache.get(MutableKey.make(Portal.class, id));
    }

    public int size() {
        return cache.size();
    }
}
