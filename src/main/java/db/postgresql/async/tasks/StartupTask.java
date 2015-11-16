package db.postgresql.async.tasks;

import db.postgresql.async.CompletableTask;
import db.postgresql.async.CommandStatus;
import db.postgresql.async.SessionInfo;
import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.TransactionStatus;
import db.postgresql.async.messages.Authentication;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.KeyData;
import db.postgresql.async.messages.ReadyForQuery;
import db.postgresql.async.messages.Response;
import java.nio.ByteBuffer;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.concurrent.CompletableFuture;

public class StartupTask extends BaseTask<KeyData> implements CompletableTask<KeyData> {

    final SessionInfo info;
    private KeyData keyData;
    private boolean authenticated = false;
    private CompletableFuture<KeyData> future;
    
    public StartupTask(final SessionInfo info) {
        this.info = info;
        this.future = new CompletableFuture<>();
    }

    private boolean readProcessor(final FrontEndMessage fe, final Response resp) {
        switch(resp.getBackEnd()) {
        case Authentication:
            authentication(fe, (Authentication) resp);
            return true;
        case BackendKeyData:
            keyData = (KeyData) resp;
            return true;
        case ReadyForQuery:
            readyForQuery = (ReadyForQuery) resp;
            future.complete(keyData);
            return false;
        default:
            throw new UnsupportedOperationException(resp.getBackEnd() + " is not a supported startup type");
        }
    }

    private void authentication(final FrontEndMessage fe, final Authentication auth) {
        switch(auth.getType()) {
        case AuthenticationOk:
            authenticated = true;
            return;
        case AuthenticationCleartextPassword:
            fe.password(info.getPassword());
            return;
        case AuthenticationMD5Password:
            fe.md5(info.getUser(), info.getPassword(), ByteBuffer.wrap(auth.getSalt()));
            return;
        default:
            throw new UnsupportedOperationException(auth.getBackEnd() + " is not a supported authentication type");
        }
    }

    public void computeNextState(final int needs) {
        if(needs > 0) {
            nextState = TaskState.needs(needs);
        }
        else if(authenticated) {
            nextState = TaskState.finished();
        }
        else {
            nextState = TaskState.write();
        }
    }

    public void onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        computeNextState(pump(readBuffer, (resp) -> readProcessor(fe, resp)));
    }

    public void onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        fe.startup(info.getInitKeysValues());
        nextState = TaskState.write();
    }

    public KeyData getResult() {
        return keyData;
    }

    public CommandStatus getCommandStatus() {
        return null;
    }

    public TransactionStatus getTransactionStatus() {
        return readyForQuery.getStatus();
    }

    public CompletableFuture<KeyData> getFuture() {
        return future;
    }
}
