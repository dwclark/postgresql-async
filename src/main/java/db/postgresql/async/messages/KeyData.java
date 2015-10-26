package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class KeyData extends Response {

    public final int pid;
    public final int secretKey;
    
    public KeyData(final ByteBuffer buffer) {
        super(buffer);
        this.pid = buffer.getInt();
        this.secretKey = buffer.getInt();
    }
}
