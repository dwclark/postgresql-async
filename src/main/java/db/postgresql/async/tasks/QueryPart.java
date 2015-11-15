package db.postgresql.async.tasks;

import db.postgresql.async.Row;
import db.postgresql.async.messages.DataRow;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class QueryPart<A> {
    public final String sql;
    public String getSql() { return sql; }
    
    public A accumulator;
    public A getAccumulator() { return accumulator; }
    
    public final BiFunction<A,Row,A> func;
    public BiFunction<A,Row,A> getFunc() { return func; }
    
    public QueryPart(final String sql, final A accumulator, final BiFunction<A,Row,A> func) {
        this.sql = sql;
        this.accumulator = accumulator;
        this.func = func;
    }

    public void onDataRow(final DataRow dataRow) {
        dataRow.with(() -> accumulator = func.apply(accumulator, dataRow));
    }

    public static QueryPart<Integer> forExecute(final String sql) {
        return new QueryPart<>(sql, Integer.valueOf(0), Row::nullExecute);
    }

    public static QueryPart<List> forList(final String sql, final BiFunction<List,Row,List> func) {
        return new QueryPart<>(sql, new ArrayList<>(), func);
    }

    public static QueryPart<Map> forMap(final String sql, final BiFunction<Map,Row,Map> func) {
        return new QueryPart<>(sql, new LinkedHashMap<>(), func);
    }
}
