package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.io.IOException;

public class CopyData extends Response {

    private int copied = 0;
    public int getCopied() { return copied; }
    public int getRemaining() { return getSize() - copied; }

    private final ByteBuffer buffer;
    
    public CopyData(final ByteBuffer buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    public void toChannel(final WritableByteChannel channel) {
        final int currentLimit = buffer.limit();
        try {
            buffer.limit(buffer.position() + getSize());
            while(getRemaining() > 0) {
                copied += channel.write(buffer);
            }
        }
        catch(IOException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            buffer.limit(currentLimit);
        }
    }
}
