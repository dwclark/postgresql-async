package db.postgresql.async.pginfo;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import db.postgresql.async.serializers.*;

public class PgTypeRegistry implements Registry {

    private static final AtomicInteger counter = new AtomicInteger(1);
    
    private final ConcurrentMap<Object,PgType> pgTypeMap = new ConcurrentHashMap<>(200, 0.75f, 1);
    private final ConcurrentMap<Object,Serializer> serializerMap = new ConcurrentHashMap<>(200, 0.75f, 1);

    public PgTypeRegistry() { }

    public PgTypeRegistry add(final PgType val) {
        pgTypeMap.put(val.getOid(), val);
        pgTypeMap.put(val.getArrayId(), val);
        pgTypeMap.put(val.getName(), val);
        return this;
    }

    public <T> PgTypeRegistry add(final Serializer<T> serializer) {
        for(String name : serializer.getPgNames()) {
            serializerMap.put(name, serializer);
            serializerMap.put(serializer.getType(), serializer);
            final PgType pgType = pgType(name);
            if(pgType != null) {
                serializerMap.put(pgType.getOid(), serializer);
            }
        }

        return this;
    }


    public PgType pgType(final Integer oid) {
        return pgTypeMap.get(oid);
    }

    public PgType pgType(final String name) {
        return pgTypeMap.get(name);
    }

    @SuppressWarnings("unchecked")
    public <T> Serializer<T> serializer(final Class<T> type) {
        return serializerMap.get(type);
    }

    public Serializer serializer(final Integer oid) {
        return serializerMap.get(oid);
    }

    public Serializer serializer(final String name) {
        return serializerMap.get(name);
    }
}
