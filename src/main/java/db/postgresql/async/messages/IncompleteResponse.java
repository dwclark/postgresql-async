package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class IncompleteResponse extends Response {

    private final int needs;
    
    public IncompleteResponse(final ByteBuffer buffer) {
        super(buffer);
        needs = getSize() - buffer.remaining();
        buffer.position(buffer.position() - BackEnd.HEADER_SIZE);
    }

    public IncompleteResponse() {
        needs = 1;
    }

    @Override
    public int getNeeds() {
        return needs;
    }
}
