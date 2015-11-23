package db.postgresql.async;

import java.util.function.Consumer;
import java.util.List;
import java.util.ArrayList;
import java.util.function.Function;

public interface Cursor {
    void move(Direction direction, int count);

    default void move(final Direction direction) {
        move(direction, Direction.IGNORE_COUNT);
    }
    
    void consume(Direction direction, int count, Consumer<Row> consumer);

    default void consume(final Direction direction, Consumer<Row> consumer) {
        consume(direction, Direction.IGNORE_COUNT, consumer);
    }

    default <T> List<T> toList(final Direction direction, int count, Function<Row,T> func) {
        final List<T> ret = new ArrayList<>();
        final Consumer<Row> consumer = (row) -> ret.add(func.apply(row));
        consume(direction, count, consumer);
        return ret;
    }

    default <T> List<T> toList(final Direction direction, final Function<Row,T> func) {
        return toList(direction, Direction.IGNORE_COUNT, func);
    }
    
    void close();
}
