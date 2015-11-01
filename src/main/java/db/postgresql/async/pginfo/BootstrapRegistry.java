package db.postgresql.async.pginfo;

public class BootstrapRegistry implements Registry {

    public PgType pgType(final Integer oid) {
        throw new UnsupportedOperationException();
    }
    
    public <T> Serializer<T> serializer(final Class<T> type) {
        throw new UnsupportedOperationException();
    }
    
    public Serializer serializer(final Integer oid) {
        throw new UnsupportedOperationException();
    }
}
