package db.postgresql.async;

import db.postgresql.async.tasks.TransactionTask;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface Transaction {
    int simple(String sql);
    <T> List<T> simple(String sql, Function<Row,T> processor);
    <T> T simple(final String sql, final T accumulator, final BiFunction<T,Row,T> processor);
    List simple(List<QueryPart<?>> parts);
    
    int prepared(String sql, List<Object> args);
    List<Integer> bulkPrepared(String sql, List<List<Object>> args);
    <T> List<T> prepared(String sql, List<Object> args, final Function<Row,T> processor);
    
    void noOutput(final String sql);
    void rollback();
    void commit();

    static <T> CompletableTask<T> single(final Concurrency concurrency, final Task<T> task) {
        return new TransactionTask.Single<>(concurrency.begin(), task);
    }

    static CompletableTask<List> multiple(final Concurrency concurrency, final Task... tasks) {
        return new TransactionTask.Multiple(concurrency.begin(), Arrays.asList(tasks));
    }
}
