package db.postgresql.async;

import db.postgresql.async.pginfo.StatementCache;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.Response;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import db.postgresql.async.tasks.*;

public interface Task<T> {

    T getResult();
    TransactionStatus getTransactionStatus();
    CommandStatus getCommandStatus();
    PostgresqlException getError();
    void onStart(FrontEndMessage fe, ByteBuffer readBuffer);
    void onRead(FrontEndMessage fe, ByteBuffer readBuffer);
    void onWrite(FrontEndMessage fe, ByteBuffer readBuffer);
    void onFail(Throwable t);
    void onTimeout(FrontEndMessage fe, ByteBuffer readBuffer);
    TaskState getNextState();
    long getTimeout();
    TimeUnit getUnits();
    void setOobHandlers(final Map<BackEnd,Consumer<Response>> oobHandlers);
    void setStatementCache(StatementCache cache);
    void executed();
    boolean isExecuted();
    
    default boolean isTerminal() {
        return false;
    }
    
    default CompletableTask<T> toCompletable() {
        if(this instanceof CompletableTask) {
            return (CompletableTask<T>) this;
        }
        else {
            return new SingleCompletableTask<T>(this);
        }
    }

    public interface Simple {

        public static final List<Object> NO_ARGS = Collections.emptyList();
        
        static Task<Integer> execute(final String sql) {
            return new SimpleTask.Execute(sql);
        }

        static <T> Task<List<T>> query(final String sql, final Function<Row,T> processor) {
            return query(sql, new ArrayList<>(), (list, row) -> {
                    list.add(processor.apply(row));
                    return list;
                });
        }

        static <T> Task<T> query(final String sql, final T accumulator, final BiFunction<T,Row,T> processor) {
            return new SimpleTask.Query<>(sql, accumulator, processor);
        }

        static Task<List<Object>> bulkExecute(final List<QueryPart<?>> parts) {
            return new SimpleTask.Multi(parts);
        }

        static Task<NullOutput> noOutput(final String sql) {
            return SimpleTask.noOutput(sql);
        }
    }

    public interface Prepared {

        public static final List<Object> NO_ARGS = Collections.emptyList();
        
        static Task<Integer> execute(final String sql, final List<Object> args) {
            return new ExecuteTask.Execute(sql, args);
        }
        
        static Task<Integer> execute(final String sql, final List<Object> args, final Consumer<Integer> consumer) {
            return new ExecuteTask.Execute(sql, args, consumer);
        }

        static Task<List<Integer>> bulkExecute(final String sql, final List<List<Object>> args) {
            return new ExecuteTask.BulkExecute(sql, args);
        }

        static <T> Task<List<T>> query(final String sql, List<Object> args, final Function<Row,T> processor) {
            final BiFunction<List<T>,Row,List<T>> biFunc = (list,row) -> { list.add(processor.apply(row)); return list; };
            return query(sql, args, new ArrayList<>(), biFunc);
        }
        
        static <T> Task<T> query(final String sql, List<Object> args, T accumulator, final BiFunction<T,Row,T> processor) {
            return new ExecuteTask<>(sql, Collections.singletonList(args), accumulator, processor);
        }

        static Task<NullOutput> rollback() {
            return AnonymousTask.rollback();
        }
    }

    static <T> TransactionTask.Builder<T> transaction() {
        return new TransactionTask.Builder<>();
    }

    static <T> TransactionTask.Builder<T> transaction(final T a) {
        return new TransactionTask.Builder<T>().accumulator(a);
    }
}
