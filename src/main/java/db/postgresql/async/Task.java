package db.postgresql.async;

import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.Notice;
import db.postgresql.async.messages.Response;
import db.postgresql.async.tasks.SingleCompletableTask;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public interface Task<T> {

    public T getResult();
    public PostgresqlException getError();
    public TaskState onStart(FrontEndMessage fe, ByteBuffer readBuffer);
    public TaskState onRead(FrontEndMessage fe, ByteBuffer readBuffer);
    public TaskState onWrite(FrontEndMessage fe, ByteBuffer readBuffer);
    public void onFail(Throwable t);
    public TaskState onTimeout(FrontEndMessage fe, ByteBuffer readBuffer);
    public long getTimeout();
    public TimeUnit getUnits();
    public void setOobHandlers(final Map<BackEnd,Consumer<Response>> oobHandlers);
    
    default public CompletableTask<T> toCompletable() {
        if(this instanceof CompletableTask) {
            return (CompletableTask<T>) this;
        }
        else {
            return new SingleCompletableTask<>(this);
        }
    }
}
