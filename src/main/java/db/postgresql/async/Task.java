package db.postgresql.async;

import db.postgresql.async.pginfo.StatementCache;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.Response;
import java.io.File;
import java.io.IOException;
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
import java.nio.channels.WritableByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public interface Task<T> {

    T getResult();
    TransactionStatus getTransactionStatus();
    CommandStatus getCommandStatus();
    Throwable getError();
    void setError(Throwable t);
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

        static Task<Integer> count(final String sql) {
            return new SimpleTask.Execute(sql);
        }
        
        static Task<Integer> count(final String sql, final Consumer<Integer> consumer) {
            return new SimpleTask.Execute(sql, consumer);
        }

        static Task<Void> acceptRows(final String sql, final Consumer<Row> processor) {
            final BiFunction<Void,Row,Void> func = (accum,row) -> { processor.accept(row); return null; };
            return applyRows(sql, null, func);
        }
        
        static <T> Task<List<T>> applyRows(final String sql, final Function<Row,T> processor) {
            final BiFunction<List<T>,Row,List<T>> func = (accum,row) -> { accum.add(processor.apply(row)); return accum; };
            return applyRows(sql, new ArrayList<>(), func);
        }
        
        static <T> Task<T> applyRows(final String sql, T accumulator, final BiFunction<T,Row,T> processor) {
            return new SimpleTask.Query<>(sql, accumulator, processor);
        }

        static Task<Void> rollback() {
            return SimpleTask.rollback();
        }

        static Task<Void> noOutput(final String sql) {
            return SimpleTask.noOutput(sql);
        }

        static Task<List<Object>> bulkExecute(final List<QueryPart<?>> parts) {
            return new SimpleTask.Multi(parts);
        }
    }

    public interface Prepared {

        public static final List<Object> NO_ARGS = Collections.emptyList();
        
        static Task<Integer> count(final String sql, final List<Object> args) {
            return new ExecuteTask.Execute(sql, args);
        }
        
        static Task<Integer> count(final String sql, final List<Object> args, final Consumer<Integer> consumer) {
            return new ExecuteTask.Execute(sql, args, consumer);
        }

        static Task<List<Integer>> bulkCount(final String sql, final List<List<Object>> args) {
            return new ExecuteTask.BulkExecute(sql, args);
        }

        static Task<Void> acceptRows(final String sql, final Consumer<Row> processor) {
            return acceptRows(sql, NO_ARGS, processor);
        }
        
        static Task<Void> acceptRows(final String sql, List<Object> args, final Consumer<Row> processor) {
            final BiFunction<Void,Row,Void> biFunc = (no,row) -> { processor.accept(row); return null; };
            return applyRows(sql, args, null, biFunc);
        }

        static <T> Task<List<T>> applyRows(final String sql, final Function<Row,T> processor) {
            return applyRows(sql, NO_ARGS, processor);
        }
        
        static <T> Task<List<T>> applyRows(final String sql, final List<Object> args, final Function<Row,T> processor) {
            final BiFunction<List<T>,Row,List<T>> biFunc = (list,row) -> { list.add(processor.apply(row)); return list; };
            return applyRows(sql, args, new ArrayList<>(), biFunc);
        }
        
        static <T> Task<T> applyRows(final String sql, final List<Object> args, final T accumulator, final BiFunction<T,Row,T> processor) {
            return new ExecuteTask<>(sql, Collections.singletonList(args), accumulator, processor);
        }

        static <T> Task<T> applyFields(final String sql, List<Object> args, final T accumulator, final BiFunction<T,Field,T> processor) {
            return new ExecuteTask<>(sql, Collections.singletonList(args), RowMode.FIELD, accumulator, null, processor);
        }

        static Task<Void> rollback() {
            return AnonymousTask.rollback();
        }

        static Task<Void> noOutput(final String sql, List<Object> args) {
            return new ExecuteTask.NoOutput(sql, args);
        }

        static Task<Void> noOutput(final String sql) {
            return new ExecuteTask.NoOutput(sql);
        }
    }

    public interface Copy {

        static Task<Long> fromServer(final String sql, final WritableByteChannel channel) {
            return new CopyFromServerTask(sql, channel);
        }

        static Task<Long> fromServer(final String sql, final File file) {
            try {
                final FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
                                                             StandardOpenOption.WRITE, StandardOpenOption.APPEND);
                return fromServer(sql, channel);
            }
            catch(IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        static Task<Long> toServer(final String sql, final ReadableByteChannel channel) {
            return new CopyToServerTask(sql, channel);
        }

        static Task<Long> toServer(final String sql, final File file) {
            try {
                final FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
                return toServer(sql, channel);
            }
            catch(IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }

    static <T> TransactionTask.Builder<T> transaction() {
        return new TransactionTask.Builder<>();
    }

    static <T> TransactionTask.Builder<T> transaction(final T a) {
        return new TransactionTask.Builder<T>().accumulator(a);
    }
}
