package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class FunctionCallResponse extends Response {

    private static final byte[] EMPTY = new byte[0];
    
    private final byte[] data;
    public byte[] getData() { return data; }

    public FunctionCallResponse(final ByteBuffer buffer) {
        super(buffer);
        final int length = buffer.getInt();
        if(length > 0) {
            this.data = new byte[length];
            buffer.get(data);
        }
        else {
            data = EMPTY;
        }
    }
}
