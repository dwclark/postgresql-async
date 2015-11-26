package db.postgresql.async.types;

public interface UdtInput {
    boolean hasNext();
    char getBeginDelimiter();
    char getEndDelimiter();
    <T> T read(Class<T> type);
    boolean readBoolean();
    short readShort();
    int readInt();
    long readLong();
    float readFloat();
    double readDouble();
}
