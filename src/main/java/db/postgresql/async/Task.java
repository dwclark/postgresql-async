package db.postgresql.async;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.Map;
import java.util.EnumMap;
import java.util.function.Consumer;
import db.postgresql.async.messages.*;
import java.util.concurrent.TimeUnit;

public abstract class Task <T> {

    private final Map<BackEnd,Consumer<Response>> oobHandlers = new EnumMap<>(BackEnd.class);
    private final CompletableFuture<T> future;
    private final long timeout;
    private final TimeUnit units;
    
    public Task() {
        this(0L, TimeUnit.SECONDS);
    }

    public Task(final long timeout, final TimeUnit units) {
        this.timeout = timeout;
        this.units = units;
        this.future = new CompletableFuture<>();
    }

    public abstract TaskState onStart(FrontEndMessage fe, ByteBuffer readBuffer);
    public abstract TaskState onRead(FrontEndMessage fe, ByteBuffer readBuffer);
    public abstract TaskState onWrite(FrontEndMessage fe, ByteBuffer readBuffer);

    public TaskState ensureRecord(final ByteBuffer buffer) {
        final int needs = BackEnd.needs(buffer);
        return (needs > 0) ? TaskState.needs(needs) : TaskState.read();
    }
    
    public void onFail(Throwable t) {
        future.completeExceptionally(t);
    }

    public TaskState onTimeout() {
        return TaskState.finished();
    }

    public void onOob(final Response r) {
        if(!r.getBackEnd().outOfBand) {
            throw new IllegalArgumentException(r.getBackEnd() + " is not an out of band type");
        }

        oobHandlers.get(r.getBackEnd()).accept(r);
    }

    public void addOobHandler(final BackEnd backEnd, final Consumer<Response> handler) {
        oobHandlers.put(backEnd, handler);
    }

    public void setOobHandlers(final Map<BackEnd,Consumer<Response>> oobHandlers) {
        this.oobHandlers.putAll(oobHandlers);
    }

    public void clearOobHandlers() {
        oobHandlers.clear();
    }
    
    public CompletableFuture<T> getFuture() { return future; }
    public long getTimeout() { return timeout; }
    public TimeUnit getUnits() { return units; }
}
