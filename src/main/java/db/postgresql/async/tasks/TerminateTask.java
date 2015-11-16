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

public class TerminateTask extends BaseTask<Void> {

    @Override
    public void onRead(final FrontEndMessage feMessage, final ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onWrite(final FrontEndMessage feMessage, final ByteBuffer buffer) {
        nextState = TaskState.terminate();
    }

    @Override
    public void onStart(final FrontEndMessage feMessage, final ByteBuffer buffer) {
        feMessage.terminate();
        nextState = TaskState.write();
    }

    public CommandStatus getCommandStatus() {
        throw new UnsupportedOperationException();
    }

    public TransactionStatus getTransactionStatus() {
        throw new UnsupportedOperationException();
    }

    public Void getResult() {
        return null;
    }
}
