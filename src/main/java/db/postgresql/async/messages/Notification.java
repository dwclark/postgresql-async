package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class Notification extends Response {

    public final int pid;
    public final String channel;
    public final String payload;

    public Notification(final ByteBuffer buffer) {
        super(buffer);
        this.pid = buffer.getInt();
        this.channel = ascii(buffer);
        this.payload = ascii(buffer);
    }
}
