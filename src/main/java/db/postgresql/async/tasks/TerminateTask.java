package db.postgresql.async.tasks;

import db.postgresql.async.Task;
import db.postgresql.async.TaskState;
import db.postgresql.async.messages.FrontEndMessage;
import java.nio.ByteBuffer;

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

    public Void getResult() {
        return null;
    }
}
