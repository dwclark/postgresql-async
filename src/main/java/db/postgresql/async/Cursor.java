package db.postgresql.async;

import java.util.Collections;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.List;
import java.util.ArrayList;
import java.util.function.Function;
import static db.postgresql.async.tasks.AnonymousTask.*;
import static db.postgresql.async.serializers.SerializationContext.*;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Cursor {

    public Cursor(final IO io, final String name) {
        this.io = io;
        this.name = name;
    }

    private int totalCalls = 0;
    public int getTotalCalls() { return totalCalls; }

    private int totalRowsProcessed = 0;
    public int getTotalRowsProcessed() { return totalRowsProcessed; }
    
    private int lastRowsProcessed = 0;
    public int getLastRowsProcessed() { return lastRowsProcessed; }
    
    private IO io;
    private String name;

    public String getName() { return name; }
    
    private CursorOp op;
    public CursorOp getOp() { return op; }

    public <T> void complete(final Task<T> task) {
        try {
            final CompletableTask<T> completable = task.toCompletable();
            io.execute(completable);
            completable.getFuture().get();
        }
        catch(InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    
    public <T> T execute(final T accumulator, final Function<Cursor,CursorOp> findNextOp,
                         final BiFunction<T,Row,T> processOp) {
        this.op = findNextOp.apply(this);
        final BiFunction<T,Row,T> wrapped = wrap(accumulator, processOp);
        
        while(op.getAction() != CursorOp.Action.CLOSE) {
            ++totalCalls;
            lastRowsProcessed = 0;
            complete(query(op.toCommand(name), accumulator, Collections.emptyList(), wrapped));
            this.op = findNextOp.apply(this);
        }

        complete(query(op.toCommand(name), accumulator, Collections.emptyList(), wrapped));
        return accumulator;
    }

    public <E> List<E> standard(final Function<Row,E> processOp) {
        final List<E> accumulator = new ArrayList<>();
        
        final Function<Cursor,CursorOp> everything = (cursor) -> {
            if(cursor.getTotalCalls() == 0) {
                return CursorOp.fetchAll();
            }
            else {
                return CursorOp.close();
            } };

        final BiFunction<List<E>,Row,List<E>> processor = (list,row) -> {
            list.add(processOp.apply(row));
            return list;
        };

        return execute(accumulator, everything, processor);
    }

    public <E> List<E> batched(final int size, final Function<Row,E> processOp) {
        final List<E> accumulator = new ArrayList<>();
        
        final Function<Cursor,CursorOp> batching = (cursor) -> {
            if(cursor.getTotalCalls() == 0 || cursor.getLastRowsProcessed() < size) {
                return CursorOp.fetch(Direction.NEXT, size);
            }
            else {
                return CursorOp.close();
            } };
        
        final BiFunction<List<E>,Row,List<E>> processor = (list,row) -> {
            list.add(processOp.apply(row));
            return list;
        };
        
        return execute(accumulator, batching, processor);
    }

    private <T> BiFunction<T,Row,T> wrap(final T accumulator, final BiFunction<T,Row,T> processOp) {
        return (accum, row) -> {
            ++lastRowsProcessed;
            ++totalRowsProcessed;
            return processOp.apply(accumulator, row); };
    }
    
    public static Cursor read(final int size, final ByteBuffer buffer, final int oid) {
        return new Cursor(io(), bufferToString(size, buffer));
    }

    public static void write(final ByteBuffer buffer, final Object o) {
        throw new UnsupportedOperationException();
    }
}
