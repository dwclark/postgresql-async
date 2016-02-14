package db.postgresql.async.tasks;

import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.TransactionStatus;
import db.postgresql.async.CommandStatus;
import java.util.concurrent.TimeUnit;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.pginfo.StatementCache;
import java.util.Map;
import db.postgresql.async.messages.Response;
import java.util.function.Consumer;
import java.nio.ByteBuffer;
import db.postgresql.async.messages.BackEnd;

public class SslTask extends BaseTask<Boolean> {

    public static final byte CONTINUE = (byte) 'S';
    public static final byte STOP = (byte) 'N';
    
    protected TaskState nextState = TaskState.start();
    protected boolean executed = false;
    protected TransactionStatus status;
    protected Throwable error;
    
    public TransactionStatus getTransactionStatus() { return status; }
    public CommandStatus getCommandStatus() { return null; }
    public Throwable getError() { return error; }
    public void setError(Throwable t) { error = t; }
    
    public void onFail(Throwable t) {
        nextState = TaskState.terminate();
    }
    
    public void onTimeout(FrontEndMessage fe, ByteBuffer readBuffer) {
        nextState = TaskState.terminate();
    }
    
    public TaskState getNextState() { return nextState; }
    
    public long getTimeout() { return 0L; }
    
    public TimeUnit getUnits() { return TimeUnit.SECONDS; }
    
    public void setOobHandlers(final Map<BackEnd,Consumer<Response>> oobHandlers) { }
    
    public void setStatementCache(StatementCache cache) { }
    
    public void executed() { executed = true; }
    
    public boolean isExecuted() { return executed; }

    private Boolean result;

    public Boolean getResult() { return result; }
        
    public void onStart(FrontEndMessage fe, ByteBuffer readBuffer) {
        fe.ssl();
        nextState = TaskState.write();
    }
        
    public void onRead(FrontEndMessage fe, ByteBuffer readBuffer) {
        if(readBuffer.hasRemaining()) {
            final byte resp = readBuffer.get();
            if(resp == CONTINUE) {
                result = Boolean.TRUE;
                status = TransactionStatus.IDLE;
            }
            else {
                result = Boolean.FALSE;
                status = TransactionStatus.FAILED;
            }

            nextState = TaskState.finished();
        }
        else {
            nextState = TaskState.read();
        }
    }
        
    public void onWrite(FrontEndMessage fe, ByteBuffer readBuffer) {
        nextState = TaskState.read();
    }
}
