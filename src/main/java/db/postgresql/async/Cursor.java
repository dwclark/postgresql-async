package db.postgresql.async;

import java.util.Collections;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.List;
import java.util.ArrayList;
import java.util.function.Function;
import db.postgresql.async.tasks.CursorTask;
import static db.postgresql.async.serializers.SerializationContext.*;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Cursor {

    public Cursor(final String name) {
        this.name = name;
    }

    private int totalCalls = 0;
    public void setTotalCalls(final int val) { totalCalls = val; }
    public void incrementTotalCalls() { ++totalCalls; }
    public int getTotalCalls() { return totalCalls; }

    private int totalRowsProcessed = 0;
    public void setTotalRowsProcessed(final int val) { totalRowsProcessed = val; }
    public void incrementTotalRowsProcessed() { ++totalRowsProcessed; }
    public int getTotalRowsProcessed() { return totalRowsProcessed; }
    
    private int lastRowsProcessed = 0;
    public void setLastRowsProcessed(final int val) { lastRowsProcessed = val; }
    public void incrementLastRowsProcessed() { ++lastRowsProcessed; }
    public int getLastRowsProcessed() { return lastRowsProcessed; }
    
    private String name;
    public String getName() { return name; }
    
    public <T> Task<T> task(final T accumulator,
                            final Function<Cursor,CursorOp> findNextOp,
                            final BiFunction<T,Row,T> processOp) {
        return new CursorTask<>(this, accumulator, findNextOp, processOp);
    }

    public static CursorOp everything(final Cursor cursor) {
        if(cursor.getTotalCalls() == 0) {
            return CursorOp.fetchAll();
        }
        else {
            return CursorOp.close();
        }
    }
    
    public Task<Void> acceptRows(final Consumer<Row> consumer) {
        final BiFunction<Void,Row,Void> processor = (v, row) -> {
            consumer.accept(row);
            return null;
        };

        return task(null, Cursor::everything, processor);
    }
    
    public <T> Task<List<T>> applyRows(final Function<Row,T> processOp) {
        final List<T> accumulator = new ArrayList<>();

        final BiFunction<List<T>,Row,List<T>> processor = (list,row) -> {
            list.add(processOp.apply(row));
            return list;
        };

        return task(accumulator, Cursor::everything, processor);
    }

    public <T> Task<List<T>> batched(final int size, final Function<Row,T> processOp) {
        final List<T> accumulator = new ArrayList<>();
        
        final Function<Cursor,CursorOp> batching = (cursor) -> {
            if(cursor.getTotalCalls() == 0 || cursor.getLastRowsProcessed() == size) {
                return CursorOp.fetch(Direction.NEXT, size);
            }
            else {
                return CursorOp.close();
            } };
        
        final BiFunction<List<T>,Row,List<T>> processor = (list,row) -> {
            list.add(processOp.apply(row));
            return list;
        };
        
        return task(accumulator, batching, processor);
    }
    
    public static Cursor read(final int size, final ByteBuffer buffer, final int oid) {
        return new Cursor(bufferToString(size, buffer));
    }

    public static void write(final ByteBuffer buffer, final Object o) {
        throw new UnsupportedOperationException();
    }
}
