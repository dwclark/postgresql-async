package db.postgresql.async;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import db.postgresql.async.messages.FrontEndMessage;
import java.util.Map;
import java.util.function.Consumer;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.Response;
import db.postgresql.async.messages.Notice;

public interface Task {

    public TaskState onStart(FrontEndMessage fe, ByteBuffer readBuffer);
    public TaskState onRead(FrontEndMessage fe, ByteBuffer readBuffer);
    public TaskState onWrite(FrontEndMessage fe, ByteBuffer readBuffer);
    public void onFail(Throwable t);
    public TaskState onTimeout(FrontEndMessage fe, ByteBuffer readBuffer);
    public CompletableFuture<?> getFuture();
    public Notice getError();
    public long getTimeout();
    public TimeUnit getUnits();
    public void setOobHandlers(final Map<BackEnd,Consumer<Response>> oobHandlers);
}
