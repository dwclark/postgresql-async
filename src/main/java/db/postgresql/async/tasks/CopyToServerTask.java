package db.postgresql.async.tasks;

import db.postgresql.async.CommandStatus;
import db.postgresql.async.PostgresqlException;
import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.TransactionStatus;
import db.postgresql.async.messages.*;
import db.postgresql.async.pginfo.StatementCache;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class CopyToServerTask extends BaseTask<Long> {

    private final String sql;
    private final ReadableByteChannel channel;
    private CopyResponse copyInResponse;
    private boolean channelComplete;
    private long total;

    public Long getResult() {
        return total;
    }
    
    public CopyToServerTask(final String sql, final ReadableByteChannel channel) {
        this.sql = sql;
        this.channel = channel;
    }

    public void onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        fe.query(sql);
        nextState = TaskState.write();
    }

    protected boolean readProcessor(final Response r) {
        switch(r.getBackEnd()) {
        case CopyInResponse:
            copyInResponse = (CopyResponse) r;
            return false;
        case CommandComplete:
            commandComplete = (CommandComplete) r;
            return true;
        case ReadyForQuery:
            readyForQuery = (ReadyForQuery) r;
            return false;
        default:
            setError(new UnsupportedOperationException("Not expecting: " + r.getBackEnd()));
            return false;
        }
    }

    protected void writePossible(final FrontEndMessage fe) {
        try {
            if(channelComplete) {
                nextState = TaskState.read();
            }
            else {
                final int written = fe.copyData(channel);
                total += written;
                if(written >= 0) {
                    nextState = TaskState.write();
                }
                else {
                    channelComplete = true;
                    fe.copyDone();
                    nextState = TaskState.write();
                }
            }
        }
        catch(Throwable t) {
            setError(t);
        }
    }
    
    public void onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        int needs = pump(readBuffer, this::readProcessor);

        if(getHasErrorResponse()) {
            return;
        }
        
        if(needs > 0) {
            nextState = TaskState.needs(needs);
        }
        else if(!channelComplete) {
            writePossible(fe);
        }
        else if(readyForQuery == null) {
            nextState = TaskState.read();
        }
        else {
            nextState = TaskState.finished();
        }
    }

    public void onWrite(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        if(getHasErrorResponse()) {
            return;
        }
        
        if(copyInResponse == null) {
            nextState = TaskState.read();
        }
        else {
            writePossible(fe);
        }
    }
}
