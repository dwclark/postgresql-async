package db.postgresql.async.messages;

public interface ExtractData {
    <T> T next(Class<T> type);
    boolean nextBoolean();
    double nextDouble();
    float nextFloat();
    int nextInt();
    long nextLong();
    short nextShort();
}
