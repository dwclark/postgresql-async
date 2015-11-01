package db.postgresql.async.pginfo;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import static db.postgresql.async.pginfo.PgType.*;
import static db.postgresql.async.pginfo.NameKey.threadLocal;
import db.postgresql.async.serializers.*;

public class PgTypeRegistry implements Registry {

    private static final IntegerSerializer iser = IntegerSerializer.instance;
    private static final StringSerializer sser = StringSerializer.instance;
    private static final AtomicInteger counter = new AtomicInteger(1);

    private final short id;
    public short getId() { return id; }
    
    private final ConcurrentMap<Integer,PgType> oidMap = new ConcurrentHashMap<>(200, 0.75f, 1);
    private final ConcurrentMap<NameKey,PgType> nameMap = new ConcurrentHashMap<>(200, 0.75f, 1);
    private final ConcurrentMap<Class,Serializer> classSerializers = new ConcurrentHashMap<>(200, 0.75f, 1);
    private final ConcurrentMap<Integer,Serializer> oidSerializers = new ConcurrentHashMap<>(200, 0.75f, 1);

    public PgTypeRegistry() {
        this.id = (short) counter.getAndIncrement();
    }

    public PgTypeRegistry add(final PgType val) {
        final PgType pgType = builder(val).registry(id).build();
        oidMap.put(pgType.getOid(), pgType);
        nameMap.put(pgType.getNameKey(), pgType);
        return this;
    }

    public PgType pgType(final Integer oid) {
        return oidMap.get(oid);
    }

    public PgType pgType(final String schema, final String name) {
        return pgType(threadLocal(schema, name));
    }

    public PgType pgType(final String fullName) {
        return pgType(threadLocal(fullName));
    }

    public PgType pgType(final NameKey key) {
        return nameMap.get(key);
    }

    @SuppressWarnings("unchecked")
    public <T> Serializer<T> serializer(final Class<T> type) {
        return classSerializers.get(type);
    }

    public Serializer serializer(final Integer oid) {
        return oidSerializers.get(oid);
    }
}
