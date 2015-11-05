package db.postgresql.async.tasks;

import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.EnumMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import db.postgresql.async.messages.*;
import java.util.concurrent.TimeUnit;

public abstract class BaseTask <T> implements Task<T> {

    private final Map<BackEnd,Consumer<Response>> oobHandlers = new EnumMap<>(BackEnd.class);
    private final CompletableFuture<T> future;
    private final long timeout;
    private final TimeUnit units;

    private Notice error;
    
    public BaseTask() {
        this(0L, TimeUnit.SECONDS);
    }

    public BaseTask(final long timeout, final TimeUnit units) {
        this.timeout = timeout;
        this.units = units;
        this.future = new CompletableFuture<>();
    }

    public Notice getError() {
        return error;
    }

    public void setError(final Notice val) {
        error = val;
    }

    public abstract TaskState onStart(FrontEndMessage fe, ByteBuffer readBuffer);
    public abstract TaskState onRead(FrontEndMessage fe, ByteBuffer readBuffer);
    
    public TaskState onWrite(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        return TaskState.read();
    }

    public TaskState ensureRecord(final ByteBuffer buffer) {
        final int needs = BackEnd.needs(buffer);
        return (needs > 0) ? TaskState.needs(needs) : TaskState.read();
    }

    public TaskState pump(final ByteBuffer readBuffer,
                          final Predicate<Response> processor,
                          final Supplier<TaskState> post) {
        TaskState readState = null;
        boolean keepGoing = true;
        while(keepGoing &&
              readBuffer.hasRemaining() &&
              (readState = ensureRecord(readBuffer)).needs == 0) {
            final Response resp = BackEnd.find(readBuffer.get()).builder.apply(readBuffer);

            if(resp.getBackEnd().outOfBand) {
                onOob(resp);
            }
            else if(resp.getBackEnd() == BackEnd.ErrorResponse) {
                onError((Notice) resp);
            }
            else {
                keepGoing = processor.test(resp);
            }
        }

        return (readState.needs > 0) ? readState : post.get(); 
    }
    
    public void onFail(Throwable t) {
        future.completeExceptionally(t);
    }

    public TaskState onTimeout(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        return TaskState.finished();
    }

    public TaskState onError(final Notice e) {
        setError(e);
        return TaskState.read();
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
