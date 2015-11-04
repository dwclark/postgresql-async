package db.postgresql.async;

import java.util.Iterator;

public interface DataRowIterator extends Iterator<Object> {
    <T> T next(Class<T> type);
    boolean nextBoolean();
    double nextDouble();
    float nextFloat();
    int nextInt();
    long nextLong();
    short nextShort();
}
