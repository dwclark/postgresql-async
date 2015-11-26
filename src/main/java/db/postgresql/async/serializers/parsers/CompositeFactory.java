package db.postgresql.async.serializers.parsers;

@FunctionalInterface
public interface CompositeFactory<T> {
    T make(char c, int level);
}
