package db.postgresql.async.pginfo;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.SortedSet;
import static db.postgresql.async.types.UdtHashing.*;
    
public class PgType {

    public static final String DEFAULT_SCHEMA = "public";
    public static final int DEFAULT_RELID = 0;
    public static final char DEFAULT_DELIMITER = ',';

    final private short registry;
    final private int oid;
    final private NameKey nameKey;
    final private int arrayId;
    final private int relId;
    final private char delimiter;
    final private SortedSet<PgAttribute> attributes;

    private PgType(final int oid, final int arrayId, final NameKey nameKey,
                   final int relId, final char delimiter, final SortedSet<PgAttribute> attributes, final short registry) {
        this.oid = oid;;
        this.arrayId = arrayId;
        this.nameKey = nameKey;
        this.relId = relId;
        this.delimiter = delimiter;
        this.attributes = attributes;
        this.registry = registry;
    }

    public static Builder builder() { return new Builder(); }
    public static Builder builder(final PgType pgType) { return new Builder(pgType); }

    public static class Builder {
        private String schema = DEFAULT_SCHEMA;
        private String name;
        private int oid;
        private int arrayId;
        private int relId = DEFAULT_RELID;
        private char delimiter = DEFAULT_DELIMITER;
        private short registry;
        private SortedSet<PgAttribute> attributes = new TreeSet<>();

        public Builder schema(final String val) { schema = val; return this; }
        public Builder name(final String val) { name = val; return this; }
        public Builder oid(final int val) { oid = val; return this; }
        public Builder arrayId(final int val) { arrayId = val; return this; }
        public Builder relId(final int val) { relId = val; return this; }
        public Builder delimiter(final char val) { delimiter = val; return this; }
        public Builder registry(final short val) { registry = val; return this; }
        public Builder attribute(final PgAttribute val) { attributes.add(val); return this; }
        
        public PgType build() {
            return new PgType(oid, arrayId, NameKey.immutable(schema, name), relId, delimiter,
                              attributes.size() == 0 ? Collections.emptySortedSet() : attributes,
                              registry);
        }

        public Builder() { }

        public Builder(final PgType pgType) {
            this.schema = pgType.getSchema();
            this.name = pgType.getName();
            this.oid = pgType.getOid();
            this.arrayId = pgType.getArrayId();
            this.relId = pgType.getRelId();
            this.delimiter = pgType.getDelimiter();
            this.attributes.addAll(pgType.getAttributes());
            this.registry = pgType.registry;
        }
    }

    public NameKey getNameKey() { return nameKey; }
    public int getOid() { return oid; }
    public String getSchema() { return nameKey.getSchema(); }
    public String getName() { return nameKey.getName(); }
    public int getArrayId() { return arrayId; }
    public int getRelId() { return relId; }
    public char getDelimiter() { return delimiter; }
    public boolean isComplex() { return relId != 0; }
    public String getFullName() { return nameKey.getFullName(); }
    public boolean isBuiltin() { return nameKey.isBuiltin(); } 

    @Override
    public boolean equals(Object rhs) {
        if(!(rhs instanceof PgType)) {
            return false;
        }

        PgType o = (PgType) rhs;
        return oid == o.oid && registry == o.registry;
    }

    @Override
    public int hashCode() {
        return hash(hash(START, oid), (int) registry);
    }

    @Override
    public String toString() {
        return new StringBuilder(256)
            .append("PgType(")
            .append("schema: " + getSchema() + ", ")
            .append("name: " + getName() + ", ")
            .append("fullName: " + getFullName() + ", ")
            .append("arrayId: " + getArrayId() + ", ")
            .append("relId: " + getRelId() + ", ")
            .append("delimiter: '" + getDelimiter() + "', ")
            .append("complex: " + isComplex() + ", ")
            .append("builtin: " + isBuiltin() + ", ")
            .append("registry: " + registry + ", ")
            .append("attributes: " + attributes + ")").toString();
    }

    public SortedSet<PgAttribute> getAttributes() {
        return attributes;
    }
}
