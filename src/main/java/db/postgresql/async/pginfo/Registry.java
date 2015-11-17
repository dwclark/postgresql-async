package db.postgresql.async.pginfo;

import db.postgresql.async.serializers.Serializer;

public interface Registry {
    PgType pgType(Integer oid);
    <T> Serializer<T> serializer(Class<T> type);
    Serializer untyped(Class type);
    Serializer serializer(Integer oid);
}
