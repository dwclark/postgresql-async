package db.postgresql.async.tasks;

import db.postgresql.async.messages.Response;
import db.postgresql.async.messages.BackEnd;
import db.postgresql.async.messages.KeyData;
import db.postgresql.async.messages.ReadyForQuery;
import db.postgresql.async.messages.Authentication;
import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.SessionInfo;
import db.postgresql.async.messages.FrontEndMessage;
import java.nio.ByteBuffer;
import java.util.function.Predicate;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.function.Function;

public class StartupTask extends BaseTask<Void> {

    final SessionInfo info;

    private KeyData keyData;
    public KeyData getKeyData() { return keyData; }

    private boolean authenticated = false;
    
    public StartupTask(final SessionInfo info) {
        this.info = info;
    }

    private boolean readProcessor(final FrontEndMessage fe, final Response resp) {
        switch(resp.getBackEnd()) {
        case Authentication:
            return authentication(fe, (Authentication) resp);
        case BackendKeyData:
            keyData = (KeyData) resp;
            return true;
        case ReadyForQuery:
            return false;
        default:
            throw new UnsupportedOperationException(resp.getBackEnd() + " is not a supported startup type");
        }
    }

    private boolean authentication(final FrontEndMessage fe, final Authentication auth) {
        switch(auth.getType()) {
        case AuthenticationOk:
            authenticated = true;
            return false;
        case AuthenticationCleartextPassword:
            fe.password(info.getPassword());
            return true;
        case AuthenticationMD5Password:
            fe.md5(info.getUser(), info.getPassword(), ByteBuffer.wrap(auth.getSalt()));
            return true;
        default:
            throw new UnsupportedOperationException(auth.getBackEnd() + " is not a supported authentication type");
        }
    }

    private TaskState nextOp() {
        return authenticated ? TaskState.read() : TaskState.write();
    }

    public TaskState onRead(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        return pump(readBuffer, (resp) -> readProcessor(fe, resp), this::nextOp);
                        
    }

    public TaskState onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        fe.startup(info.getInitKeysValues());
        return TaskState.write();
    }
}
