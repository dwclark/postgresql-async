package db.postgresql.async.serializers.parsers;

public interface CompositeFactory<T> {
    T make(char c, int level);
}
