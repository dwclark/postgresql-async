package db.postgresql.async;

import java.util.function.Consumer;

public interface Cursor {
    void move(Direction direction, int count);
    void fetch(Direction direction, int count, Consumer<Row> consumer);
    void close();
}
