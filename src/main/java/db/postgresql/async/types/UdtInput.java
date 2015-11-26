package db.postgresql.async.types;

public interface UdtInput {
    boolean hasNext();
    char getCurrentDelimiter();
    <T> T read(Class<T> type);
    boolean readBoolean(String str);
    short readShort(String str);
    int readInt(String str);
    long readLong(String str);
    float readFloat(String str);
    double readDouble(String str);
}
