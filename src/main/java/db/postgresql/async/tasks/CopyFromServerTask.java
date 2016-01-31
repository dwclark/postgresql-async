package db.postgresql.async.tasks;

import db.postgresql.async.CommandStatus;
import db.postgresql.async.PostgresqlException;
import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.TransactionStatus;
import db.postgresql.async.messages.*;
import db.postgresql.async.pginfo.StatementCache;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class CopyFromServerTask extends BaseTask<Long> {

    private final String sql;
    private final WritableByteChannel channel;
    private CopyResponse copyOutResponse;
    private boolean copyDone;
    private long total;

    public Long getResult() {
        return total;
    }
    
    public CopyFromServerTask(final String sql, final WritableByteChannel channel) {
        this.sql = sql;
        this.channel = channel;
    }

    public void onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        fe.query(sql);
        nextState = TaskState.write();
    }

    protected boolean readProcessor(final Response r) {
        switch(r.getBackEnd()) {
        case CopyOutResponse:
            copyOutResponse = (CopyResponse) r;
            return true;
        case CopyData:
            CopyData cd = (CopyData) r;
            cd.toChannel(channel);
        case CopyDone:
            try {
                channel.close();
            }
            catch(IOException ioe) {
                setError(ioe);
            }
            copyDone = true;
            return true;
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

    public void onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        int needs = pump(readBuffer, this::readProcessor);
        if(needs > 0) {
            nextState = TaskState.needs(needs);
        }
        else if(readyForQuery == null) {
            nextState = TaskState.read();
        }
        else {
            setError(new IllegalStateException());
        }
    }

    public void onWrite(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        if(copyOutResponse == null) {
            nextState = TaskState.read();
        }
        else {
            setError(new IllegalStateException());
        }
    }
}
