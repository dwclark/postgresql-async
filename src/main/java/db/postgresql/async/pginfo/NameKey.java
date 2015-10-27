package db.postgresql.async.pginfo;

import static db.postgresql.async.types.UdtHashing.*;

public abstract class NameKey {

    public abstract String getSchema();
    public abstract String getName();

    @Override
    public boolean equals(Object rhs) {
        if(!(rhs instanceof NameKey)) {
            return false;
        }
        
        NameKey o = (NameKey) rhs;
        return (getSchema().equals(o.getSchema()) &&
                getName().equals(o.getName()));
    }
    
    @Override
    public int hashCode() {
        return hash(hash(START, getSchema()), getName());
    }

    public boolean isBuiltin() {
        return "".equals(getSchema());
    }
    
    public boolean hasSchema() {
        return !isBuiltin();
    }
    
    public String getFullName() {
        if(hasSchema()) {
            return getSchema() + '.' + getName();
        }
        else {
            return getName();
        }
    }

    private static String filterSchema(String schema) {
        if(schema.equals("public") || schema.equals("pg_catalog")) {
            return "";
        }
        else {
            return schema;
        }
    }

    private static class Immutable extends NameKey {
        private final String schema;
        private final String name;

        public Immutable(final String schema, final String name) {
            this.schema = filterSchema(schema);
            this.name = name;
        }

        public String getSchema() { return schema; }
        public String getName() { return name; }
    }

    private static class Mutable extends NameKey {
        private String schema;
        private String name;
        
        public String getSchema() { return schema; }
        public String getName() { return name; }
    }

    private static final ThreadLocal<Mutable> tlMutable = new ThreadLocal<Mutable>() {
            @Override protected Mutable initialValue() {
                return new Mutable();
            }
        };

    public static NameKey immutable(final String schema, final String name) {
        return new Immutable(filterSchema(schema), name);
    }

    public static NameKey immutable(final String fullName) {
        final String[] ary = fullName.split("\\.");
        return (ary.length == 1) ? immutable("", ary[0]) : immutable(ary[0], ary[1]);
    }

    public static NameKey threadLocal(final String schema, final String name) {
        Mutable mut = tlMutable.get();
        mut.schema = filterSchema(schema);
        mut.name = name;
        return mut;
    }

    public static NameKey threadLocal(final String fullName) {
        final String[] ary = fullName.split("\\.");
        return (ary.length == 1) ? threadLocal("", ary[0]) : threadLocal(ary[0], ary[1]);
    }
}
