package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class Notification extends Response {

    private final int pid;
    public int getPid() { return pid; }
    
    private final String channel;
    public String getChannel() { return channel; }
    
    private final String payload;
    public String getPayload() { return payload; }

    public Notification(final ByteBuffer buffer) {
        super(buffer);
        this.pid = buffer.getInt();
        this.channel = ascii(buffer);
        this.payload = ascii(buffer);
    }
}
