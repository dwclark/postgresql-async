package db.postgresql.async.types;

import static db.postgresql.async.types.Hashing.*;
import db.postgresql.async.pginfo.PgAttribute;
import db.postgresql.async.pginfo.PgType;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Iterator;
import java.nio.ByteBuffer;
import db.postgresql.async.messages.Format;
import static db.postgresql.async.serializers.SerializationContext.registry;

public class Record implements SortedMap<PgAttribute,Object> {

    final PgType pgType;
    final SortedMap<PgAttribute,Object> map;

    private SortedMap<PgAttribute,Object> transfer(final Map<PgAttribute,Object> map) {
        final SortedMap<PgAttribute,Object> ret = new TreeMap<>();
        
        for(PgAttribute attr : pgType.getAttributes()) {
            if(map.containsKey(attr)) {
                ret.put(attr, map.get(attr));
            }
            else {
                ret.put(attr, null);
            }
        }

        return ret;
    }
    
    public Record(final PgType pgType, final Map<PgAttribute,Object> map) {
        this.pgType = pgType;
        this.map = transfer(map);
    }

    public Record(final PgType pgType, final SortedMap<PgAttribute,Object> map) {
        this.pgType = pgType;
        this.map = transfer(map);
    }

    public Record(final Record record) {
        this.pgType = record.pgType;
        this.map = record.map;
    }
    
    public PgType getPgType() {
        return pgType;
    }

    public Comparator<? super PgAttribute> comparator() {
        return null;
    }
    
    public Set<Map.Entry<PgAttribute,Object>> entrySet() {
        return map.entrySet();
    }

    public PgAttribute firstKey() {
        return map.firstKey();
    }

    public SortedMap<PgAttribute,Object> headMap(final PgAttribute toKey) {
        return map.headMap(toKey);
    }

    public PgAttribute lastKey() {
        return map.lastKey();
    }
    
    public SortedMap<PgAttribute,Object> subMap(final PgAttribute from, final PgAttribute to) {
        return map.subMap(from, to);
    }

    public SortedMap<PgAttribute,Object> tailMap(final PgAttribute from) {
        return map.tailMap(from);
    }

    public Collection<Object> values() {
        return map.values();
    }

    public void clear() {
        throw new UnsupportedOperationException();
    }

    public boolean containsKey(final Object o) {
        return map.containsKey(o);
    }

    public boolean containsValue(final Object o) {
        return map.containsValue(o);
    }

    @Override
    public boolean equals(final Object rhs) {
        return (rhs instanceof Record) ? equals((Record) rhs) : false;
    }

    public boolean equals(final Record rhs) {
        return pgType.equals(rhs.pgType) && map.equals(rhs.map);
    }

    public Object get(final Object key) {
        return map.get(key);
    }

    public int hashCode() {
        return hash(hash(START, pgType), map);
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public Set<PgAttribute> keySet() {
        return map.keySet();
    }

    public Object put(final PgAttribute attr, final Object val) {
        throw new UnsupportedOperationException();
    }

    public void putAll(final Map<? extends PgAttribute, ? extends Object> map) {
        throw new UnsupportedOperationException();
    }

    public Object remove(final Object key) {
        throw new UnsupportedOperationException();
    }

    public int size() {
        return map.size();
    }
    
    public Record(final PgType pgType, final ByteBuffer buffer) {
        this.pgType = pgType;
        final int columns = buffer.getInt();

        if(columns != pgType.getAttributes().size()) {
            throw new IllegalStateException("Stale data in attribute definition");
        }

        this.map = new TreeMap<>();
        final Iterator<PgAttribute> iter = pgType.getAttributes().iterator();
        for(int i = 0; i < columns; ++i) {
            PgAttribute attr = iter.next();
            final int oid = buffer.getInt();
            final PgType colPgType = registry().pgType(oid);
            map.put(attr, colPgType.read(buffer, oid));
        }
    }

    public void toBuffer(final ByteBuffer buffer) {
        buffer.putInt(size());
        for(Map.Entry<PgAttribute,Object> entry : entrySet()) {
            final PgAttribute attr = entry.getKey();
            buffer.putInt(attr.getTypeId());
            final PgType colPgType = registry().pgType(attr.getTypeId());
            colPgType.write(buffer, entry.getValue());
        }
    }

    public static Record read(final int size, final ByteBuffer buffer, final int oid) {
        return new Record(registry().pgType(oid), buffer);
    }

    public static void write(final ByteBuffer buffer, final Object o) {
        ((Record) o).toBuffer(buffer);
    }
}
