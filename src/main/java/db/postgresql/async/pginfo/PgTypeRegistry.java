package db.postgresql.async.pginfo;

import static java.util.Collections.emptyList;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import db.postgresql.async.Mapping;
import db.postgresql.async.Session;
import db.postgresql.async.Row;
import db.postgresql.async.Transaction;
import db.postgresql.async.types.Record;
import db.postgresql.async.CompletableTask;
import db.postgresql.async.Task;
import static db.postgresql.async.Task.Prepared.*;

public class PgTypeRegistry implements Registry {

    private static final AtomicInteger counter = new AtomicInteger(1);
    
    private final ConcurrentMap<Object,PgType> pgTypeMap = new ConcurrentHashMap<>(1_000, 0.75f, 1);

    public PgTypeRegistry add(final PgType val) {
        pgTypeMap.put(val.getOid(), val);
        pgTypeMap.put(val.getArrayId(), val);
        pgTypeMap.put(val.getName(), val);
        pgTypeMap.put(val.getType(), val);
        return this;
    }

    public PgType pgType(final Integer oid) {
        return pgTypeMap.get(oid);
    }

    public PgType pgType(final String name) {
        return pgTypeMap.get(name);
    }

    public PgType pgType(final Class type) {
        PgType pgType = pgTypeMap.get(type);
        if(pgType != null) {
            return pgType;
        }

        if(type.isArray()) {
            return populateArrayType(type);
        }

        return null;
    }

    private PgType populateArrayType(final Class type) {
        final Class elementType = ArrayInfo.elementType(type);
        if(elementType.isPrimitive()) {
            final Class wrapper = findWrapper(elementType);
            pgTypeMap.put(type, pgType(wrapper));
        }
        else {
            pgTypeMap.put(type, pgTypeMap.get(elementType));
        }
        
        return pgTypeMap.get(type);
    }

    private static Class findWrapper(final Class c) {
        //char, byte, void not needed
        if(c == boolean.class) {
            return Boolean.class;
        }
        else if(c == double.class) {
            return Double.class;
        }
        else if(c == float.class) {
            return Float.class;
        }
        else if(c == int.class) {
            return Integer.class;
        }
        else if(c == long.class) {
            return Long.class;
        }
        else if(c == short.class) {
            return Short.class;
        }
        else {
            return null;
        }
    }
    
    private Void extractAttribute(final Map<Integer,SortedSet<PgAttribute>> map, final Row row) {
        row.with(() -> {
                Row.Iterator iter = row.iterator();
                final PgAttribute attr = new PgAttribute(iter.nextInt(), iter.nextString(),
                                                         iter.nextInt(), iter.nextShort());
                if(!map.containsKey(attr.getRelId())) {
                    map.put(attr.getRelId(), new TreeSet<>());
                }

                map.get(attr.getRelId()).add(attr); });
        return null;
    }

    private Void extractPgType(final Map<Integer,SortedSet<PgAttribute>> attributes,
                               final Row row, final List<Mapping> mappings) {
        row.with(() -> {
                Row.Iterator iter = row.iterator();
                final int oid = iter.nextInt();
                final String name = iter.nextString() + "." + iter.nextString();
                final int arrayId = iter.nextInt();
                final int relId = iter.nextInt();
                final SortedSet<PgAttribute> myAttributes = attributes.get(relId);

                Mapping mapping = null;
                final Optional<Mapping> opt = mappings.stream().filter((m) -> m.name.equals(name)).findFirst();
                if(opt.isPresent()) {
                    mapping = opt.get();
                }
                else if(myAttributes != null) {
                    mapping = new Mapping(Record.class, name, Record::write, Record::read);
                }

                if(mapping != null) {
                    add(new PgType.Builder()
                        .oid(oid)
                        .mapping(mapping)
                        .arrayId(arrayId)
                        .relId(relId)
                        .attributes(myAttributes).build());
                } });
        
        return null;
    }
    
    public void loadTypes(final Session session) {
        final String sqlAttributes = "select attrelid, attname, atttypid, attnum " +
            "from pg_attribute where attnum >= 1 " +
            "order by attrelid asc, atttypid asc";

        final String sqlTypes = "select typ.oid, ns.nspname, typ.typname, typ.typarray, typ.typrelid " +
            "from pg_type typ " +
            "join pg_namespace ns on typ.typnamespace = ns.oid";

        final List<Mapping> mappings = session.getSessionInfo().getMappings();
        final Map<Integer,SortedSet<PgAttribute>> attributes = new HashMap<>();
        final CompletableTask<Void> task = Task.<Void>transaction(null)
            .then((none) -> query(sqlAttributes, NO_ARGS, null, (n,row) -> extractAttribute(attributes, row)))
            .then((none) -> query(sqlTypes, NO_ARGS, null, (n,row) -> extractPgType(attributes, row, mappings)))
            .build();

        try {
            session.execute(task).get();
        }
        catch(ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
