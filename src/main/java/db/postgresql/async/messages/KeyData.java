package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class KeyData extends Response {

    private final int pid;
    public int getPid() { return pid; }
    
    private final int secretKey;
    public int getSecretKey() { return secretKey; }
    
    public KeyData(final ByteBuffer buffer) {
        super(buffer);
        this.pid = buffer.getInt();
        this.secretKey = buffer.getInt();
    }
}
