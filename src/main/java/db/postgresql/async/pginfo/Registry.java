package db.postgresql.async.pginfo;

import db.postgresql.async.serializers.Serializer;

public interface Registry {
    PgType pgType(Integer oid);
    PgType pgType(String name);
    <T> Serializer<T> serializer(Class<T> type);
    Serializer serializer(Integer oid);
}
