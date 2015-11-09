package db.postgresql.async.tasks;

import java.util.function.BiFunction;
import db.postgresql.async.Row;

public class MultiQueryPart<A> {
    public final String sql;
    public A accumulator;
    public final BiFunction<A,Row,A> func;
    
    public MultiQueryPart(final String sql, final A accumulator, final BiFunction<A,Row,A> func) {
        this.sql = sql;
        this.accumulator = accumulator;
        this.func = func;
    }
}
