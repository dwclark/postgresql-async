package db.postgresql.async.tasks;

import db.postgresql.async.CompletableTask;
import db.postgresql.async.Cursor;
import db.postgresql.async.Direction;
import db.postgresql.async.IO;
import db.postgresql.async.Row;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.DataRow;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.ReadyForQuery;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class CursorTask extends SimpleTask<Void> implements CompletableTask<Void>, Cursor {

    private final IO io;
    private final String name;
    
    private String command;
    private Consumer<Row> consumer;
    private CompletableFuture<Void> future;

    public CursorTask(final IO io, final String name) {
        super("", null);
        this.io = io;
        this.name = name;
    }

    public void complete() {
        try {
            future.get();
        }
        catch(InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void move(final Direction direction, final int count) {
        command = direction.toCommand("move", count, name);
        future = new CompletableFuture<>();
        io.execute(this);
        complete();
    }

    public void consume(final Direction direction, final int count, final Consumer<Row> consumer) {
        this.consumer = consumer;
        future = new CompletableFuture<>();
        command = direction.toCommand("fetch", count, name);
        io.execute(this);
        complete();
    }

    public void close() {
        future = new CompletableFuture<>();
        command = String.format("close %s;", name);
        io.execute(this);
        complete();
    }

    @Override
    public String getSql() {
        return command;
    }

    @Override
    public void onDataRow(final DataRow row) {
        consumer.accept(row);
    }

    @Override
    public boolean onReadyForQuery(final ReadyForQuery readyForQuery) {
        future.complete(null);
        return super.onReadyForQuery(readyForQuery);
    }

    public Void getResult() {
        return null;
    }

    public CompletableFuture<Void> getFuture() {
        return future;
    }
}
