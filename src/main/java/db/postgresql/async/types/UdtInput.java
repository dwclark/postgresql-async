package db.postgresql.async.types;

public interface UdtInput {
    boolean hasNext();
    char getCurrentDelimiter();
    <T> T read(Class<T> type);
    boolean readBoolean();
    short readShort();
    int readInt();
    long readLong();
    float readFloat();
    double readDouble();
}
