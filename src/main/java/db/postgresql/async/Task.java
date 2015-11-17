package db.postgresql.async;

import db.postgresql.async.pginfo.PgSessionCache;
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
    
    default CompletableTask<T> toCompletable() {
        if(this instanceof CompletableTask) {
            return (CompletableTask<T>) this;
        }
        else {
            return new SingleCompletableTask<T>(this);
        }
    }

    default Task<Void> prepareTask(final PgSessionCache cache) {
        return null;
    }
}
