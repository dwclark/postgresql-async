package db.postgresql.async.pginfo;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.SortedSet;
import static db.postgresql.async.types.UdtHashing.*;
    
public class PgType {

    public static final int DEFAULT_RELID = 0;
    public static final char DEFAULT_DELIMITER = ',';

    final private int oid;
    final private String name;
    final private int arrayId;
    final private int relId;
    final private char delimiter;
    final private SortedSet<PgAttribute> attributes;

    private PgType(final int oid, final int arrayId, final String name,
                   final int relId, final char delimiter, final SortedSet<PgAttribute> attributes) {
        this.oid = oid;;
        this.arrayId = arrayId;
        this.name = name;
        this.relId = relId;
        this.delimiter = delimiter;
        this.attributes = attributes;
    }

    public static Builder builder() { return new Builder(); }
    public static Builder builder(final PgType pgType) { return new Builder(pgType); }

    public static class Builder {
        private String name;
        private int oid;
        private int arrayId;
        private int relId = DEFAULT_RELID;
        private char delimiter = DEFAULT_DELIMITER;
        private SortedSet<PgAttribute> attributes = new TreeSet<>();

        public Builder name(final String val) { name = val; return this; }
        public Builder oid(final int val) { oid = val; return this; }
        public Builder arrayId(final int val) { arrayId = val; return this; }
        public Builder relId(final int val) { relId = val; return this; }
        public Builder delimiter(final char val) { delimiter = val; return this; }
        public Builder attribute(final PgAttribute val) { attributes.add(val); return this; }
        public Builder attributes(final SortedSet<PgAttribute> val) { attributes = val; return this; }
        
        public PgType build() {
            return new PgType(oid, arrayId, name, relId, delimiter,
                              (attributes == null || attributes.size() == 0) ?
                              Collections.emptySortedSet() :
                              Collections.unmodifiableSortedSet(attributes));
        }

        public Builder() { }

        public Builder(final PgType pgType) {
            this.name = pgType.getName();
            this.oid = pgType.getOid();
            this.arrayId = pgType.getArrayId();
            this.relId = pgType.getRelId();
            this.delimiter = pgType.getDelimiter();
            this.attributes.addAll(pgType.getAttributes());
        }
    }

    public int getOid() { return oid; }
    public String getName() { return name; }
    public int getArrayId() { return arrayId; }
    public int getRelId() { return relId; }
    public char getDelimiter() { return delimiter; }
    public boolean isComplex() { return relId != 0; }

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
            .append("delimiter: '" + getDelimiter() + "', ")
            .append("complex: " + isComplex() + ", ")
            .append("attributes: " + attributes + ")").toString();
    }

    public SortedSet<PgAttribute> getAttributes() {
        return attributes;
    }
}
