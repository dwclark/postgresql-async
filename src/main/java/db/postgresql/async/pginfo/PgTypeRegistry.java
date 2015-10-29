package db.postgresql.async.pginfo;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import static db.postgresql.async.pginfo.PgType.*;
import static db.postgresql.async.pginfo.NameKey.threadLocal;

public class PgTypeRegistry {

    private static final AtomicInteger counter = new AtomicInteger(1);

    private final short id;
    public short getId() { return id; }
    
    private final ConcurrentMap<Integer,PgType> oidMap = new ConcurrentHashMap<>(200, 0.75f, 1);
    private final ConcurrentMap<NameKey,PgType> nameMap = new ConcurrentHashMap<>(200, 0.75f, 1);

    public PgTypeRegistry() {
        this.id = (short) counter.getAndIncrement();
    }

    public PgTypeRegistry add(final PgType val) {
        final PgType pgType = builder(val).registry(id).build();
        oidMap.put(pgType.getOid(), pgType);
        nameMap.put(pgType.getNameKey(), pgType);
        return this;
    }

    public PgType byOid(final Integer oid) {
        return oidMap.get(oid);
    }

    public PgType byName(final String schema, final String name) {
        return byName(threadLocal(schema, name));
    }

    public PgType byName(final String fullName) {
        return byName(threadLocal(fullName));
    }

    public PgType byName(final NameKey key) {
        return nameMap.get(key);
    }
}
