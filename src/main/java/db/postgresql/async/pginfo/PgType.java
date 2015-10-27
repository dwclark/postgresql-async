package db.postgresql.async.pginfo;

import java.util.SortedSet;

public class PgType {

    public static final String DEFAULT_SCHEMA = "public";
    public static final int DEFAULT_RELID = 0;
    public static final char DEFAULT_DELIMITER = ',';
    
    final private int oid;
    final private NameKey nameKey;
    final private int arrayId;
    final private int relId;
    final private char delimiter;

    private PgType(final int oid, final int arrayId, final NameKey nameKey,
                   final int relId, final char delimiter) {
        this.oid = oid;;
        this.arrayId = arrayId;
        this.nameKey = nameKey;
        this.relId = relId;
        this.delimiter = delimiter;
    }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private String schema = DEFAULT_SCHEMA;
        private String name;
        private int oid;
        private int arrayId;
        private int relId = DEFAULT_RELID;
        private char delimiter = DEFAULT_DELIMITER;

        public Builder schema(final String val) { schema = val; return this; }
        public Builder name(final String val) { name = val; return this; }
        public Builder oid(final int val) { oid = val; return this; }
        public Builder arrayId(final int val) { arrayId = val; return this; }
        public Builder relId(final int val) { relId = val; return this; }
        public Builder delimiter(final char val) { delimiter = val; return this; }

        public PgType build() {
            return new PgType(oid, arrayId, NameKey.immutable(schema, name), relId, delimiter);
        }

        public Builder() { }

        public Builder(final PgType pgType) {
            this.schema = pgType.getSchema();
            this.name = pgType.getName();
            this.oid = pgType.getOid();
            this.arrayId = pgType.getArrayId();
            this.relId = pgType.getRelId();
            this.delimiter = pgType.getDelimiter();
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
            .append("schema: " + getSchema() + ", ")
            .append("name: " + getName() + ", ")
            .append("fullName: " + getFullName() + ", ")
            .append("arrayId: " + getArrayId() + ", ")
            .append("relId: " + getRelId() + ", ")
            .append("delimiter: '" + getDelimiter() + "', ")
            .append("complex: " + isComplex() + ", ")
            .append("builtin: " + isBuiltin() + ", ")
            .append("attributes: " + getAttributes() + ")").toString();
    }

    public SortedSet<PgAttribute> getAttributes() {
        if(isComplex()) {
            throw new UnsupportedOperationException();
        }
        else {
            return null;
        }
    }
}

