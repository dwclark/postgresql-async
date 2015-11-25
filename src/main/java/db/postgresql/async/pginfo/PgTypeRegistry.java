package db.postgresql.async.pginfo;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import db.postgresql.async.Session;
import db.postgresql.async.Row;
import db.postgresql.async.tasks.SimpleTask;

import db.postgresql.async.serializers.*;

public class PgTypeRegistry implements Registry {

    private static final AtomicInteger counter = new AtomicInteger(1);
    
    private final ConcurrentMap<Object,PgType> pgTypeMap = new ConcurrentHashMap<>(200, 0.75f, 1);
    private final ConcurrentMap<Object,Serializer<?>> serializerMap = new ConcurrentHashMap<>(200, 0.75f, 1);

    public PgTypeRegistry() { }

    public PgTypeRegistry add(final PgType val) {
        pgTypeMap.put(val.getOid(), val);
        pgTypeMap.put(val.getArrayId(), val);
        pgTypeMap.put(val.getName(), val);
        return this;
    }

    public PgTypeRegistry add(final Serializer<?> serializer) {
        for(String name : serializer.getPgNames()) {
            serializerMap.put(name, serializer);
            serializerMap.put(serializer.getType(), serializer);
            serializerMap.put(serializer.getArrayType(), serializer);
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
        return (Serializer<T>) serializerMap.get(type);
    }

    public Serializer serializer(final Integer oid) {
        return serializerMap.get(oid);
    }

    public Serializer serializer(final String name) {
        return serializerMap.get(name);
    }

    private Map<Integer,SortedSet<PgAttribute>> extractAttribute(final Map<Integer,SortedSet<PgAttribute>> map, final Row row) {
        row.with(() -> {
                Row.Iterator iter = row.iterator();
                final PgAttribute attr = new PgAttribute(iter.nextInt(), iter.nextString(),
                                                         iter.nextInt(), iter.nextShort());
                if(!map.containsKey(attr.getRelId())) {
                    map.put(attr.getRelId(), new TreeSet<>());
                }

                map.get(attr.getRelId()).add(attr); });
        
        return map;
    }

    private Map<Integer,SortedSet<PgAttribute>> loadAttributes(final Session session) throws InterruptedException, ExecutionException {
        final Map<Integer,SortedSet<PgAttribute>> ret = new HashMap<>();
        return session.execute(SimpleTask
                               .query("select attrelid, attname, atttypid, attnum " +
                                      "from pg_attribute where attnum >= 1 " +
                                      "order by attrelid asc, atttypid asc",
                                      ret, this::extractAttribute).toCompletable()).get();
    }

    private Void extractPgType(final Map<Integer,SortedSet<PgAttribute>> attributes, final Row row) {
        row.with(() -> {
                Row.Iterator iter = row.iterator();
                PgType.Builder builder = new PgType.Builder()
                    .oid(iter.nextInt())
                    .name(iter.nextString() + "." + iter.nextString())
                    .arrayId(iter.nextInt());
                final int relId = iter.nextInt();
                PgType pgType = builder
                    .relId(relId)
                    .delimiter(iter.nextString().charAt(0))
                    .attributes(attributes.get(relId)).build();
                add(pgType); });
        
        return null;
    }
    
    public void loadTypes(final Session session) {
        final String sql = "select typ.oid, ns.nspname, typ.typname, typ.typarray, typ.typrelid, typ.typdelim " +
            "from pg_type typ " +
            "join pg_namespace ns on typ.typnamespace = ns.oid";
        
        try {
            final Map<Integer,SortedSet<PgAttribute>> attributes = loadAttributes(session);
            SimpleTask<Void> task = SimpleTask
                .query(sql, null, (none,row) -> extractPgType(attributes, row));
            session.execute(task.toCompletable()).get();
        }
        catch(InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        for(Serializer<?> s : session.getSessionInfo().getSerializers()) {
            add(s);
        }
    }
}
