package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class IncompleteResponse extends Response {

    private final int needs;
    
    public IncompleteResponse(final ByteBuffer buffer) {
        super(buffer);
        needs = buffer.remaining() - getSize();
        buffer.position(buffer.position() - BackEnd.HEADER_SIZE);
    }

    @Override
    public int getNeeds() {
        return needs;
    }

    @Override
    public boolean isFinished() {
        return false;
    }
}
