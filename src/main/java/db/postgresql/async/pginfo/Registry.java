package db.postgresql.async.pginfo;

public interface Registry {
    PgType pgType(Integer oid);
    PgType pgType(String name);
    PgType pgType(Class type);
    
    boolean streamable(Integer oid);
    boolean streamable(String name);
}
