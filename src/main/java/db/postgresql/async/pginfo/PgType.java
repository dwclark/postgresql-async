package db.postgresql.async.pginfo;

import db.postgresql.async.Mapping;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import static db.postgresql.async.buffers.BufferOps.*;

public class PgType {
    
    public static final int DEFAULT_RELID = 0;

    public Object read(final ByteBuffer buffer, final int oid) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(this.oid == oid) {
            return mapping.reader.read(size, buffer, oid);
        }
        else if(this.arrayId == oid) {
            return arrayRead(size, buffer);
        }
        else {
            throw new UnsupportedOperationException("Can't handle oid: " + oid);
        }
    }

    public Object read(final ByteBuffer buffer, final int oid, final Class elementType) {
        final int size = buffer.getInt();
        if(size == -1) {
            return null;
        }
        
        if(this.oid == oid) {
            return mapping.reader.read(size, buffer, oid);
        }
        else if(this.arrayId == oid) {
            return arrayRead(size, buffer, elementType);
        }
        else {
            throw new UnsupportedOperationException("Can't handle oid: " + oid);
        }
    }

    public void write(final ByteBuffer buffer, final Object o) {
        if(o == null) {
            putNull(buffer);
            return;
        }

        putWithSize(buffer, (b) -> {
                final Class objectType = o.getClass();
                if(objectType.isArray() && objectType != byte[].class) {
                    arrayWrite(b, o);
                }
                else {
                    mapping.writer.write(b, o);
                }
            });
    }

    final private Mapping mapping;
    final private int oid;
    final private int arrayId;
    final private int relId;
    final private SortedSet<PgAttribute> attributes;

    private PgType(final int oid, final int arrayId,
                   final int relId, final SortedSet<PgAttribute> attributes,
                   final Mapping mapping) {
        this.oid = oid;;
        this.arrayId = arrayId;
        this.relId = relId;
        this.attributes = attributes;
        this.mapping = mapping;
    }

    public static Builder builder() { return new Builder(); }
    public static Builder builder(final PgType pgType) { return new Builder(pgType); }

    public static class Builder {
        private int oid;
        private int arrayId;
        private int relId = DEFAULT_RELID;
        private SortedSet<PgAttribute> attributes = new TreeSet<>();
        private Mapping mapping;

        public Builder oid(final int val) { oid = val; return this; }
        public Builder arrayId(final int val) { arrayId = val; return this; }
        public Builder relId(final int val) { relId = val; return this; }
        public Builder attribute(final PgAttribute val) { attributes.add(val); return this; }
        public Builder attributes(final SortedSet<PgAttribute> val) { attributes = val; return this; }
        public Builder mapping(final Mapping val) { mapping = val; return this; }
        
        public PgType build() {
            return new PgType(oid, arrayId, relId,
                              (attributes == null || attributes.size() == 0) ?
                              Collections.emptySortedSet() :
                              Collections.unmodifiableSortedSet(attributes),
                              mapping);
        }

        public Builder() { }

        public Builder(final PgType pgType) {
            this.oid = pgType.getOid();
            this.arrayId = pgType.getArrayId();
            this.relId = pgType.getRelId();
            this.attributes.addAll(pgType.getAttributes());
            this.mapping = mapping;
        }
    }

    public int getOid() { return oid; }
    public String getName() { return mapping.name; }
    public Class getType() { return mapping.type; }
    public int getArrayId() { return arrayId; }
    public int getRelId() { return relId; }
    public boolean isComplex() { return relId != 0; }
    public boolean array(final int id) { return arrayId == id; }
    public boolean simple(final int id) { return oid == id; }
    
    @Override
    public boolean equals(Object rhs) {
        if(!(rhs instanceof PgType)) {
            return false;
        }

        PgType o = (PgType) rhs;
        return oid == o.oid;
    }

    @Override
    public int hashCode() {
        return oid;
    }

    @Override
    public String toString() {
        return new StringBuilder(256)
            .append("PgType(")
            .append("name: " + getName() + ", ")
            .append("arrayId: " + getArrayId() + ", ")
            .append("relId: " + getRelId() + ", ")
            .append("complex: " + isComplex() + ", ")
            .append("attributes: " + attributes + ")").toString();
    }

    public SortedSet<PgAttribute> getAttributes() {
        return attributes;
    }

    private Object arrayRead(final int size, final ByteBuffer buffer) {
        return arrayRead(size, buffer, mapping.type);
    }

    private Object arrayRead(final int size, final ByteBuffer buffer, final Class elementType) {
        return new ArrayInfo(this, buffer, elementType).getAry();
    }

    private void arrayWrite(final ByteBuffer buffer, final Object ary) {
        new ArrayInfo(this, ary).toBuffer(buffer);
    }
}
