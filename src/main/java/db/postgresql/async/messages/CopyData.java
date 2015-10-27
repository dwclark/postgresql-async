package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.io.IOException;

public class CopyData extends Response {

    private int copied = 0;
    public int getCopied() { return copied; }
    public int getRemaining() { return getSize() - copied; }
    
    public CopyData(final ByteBuffer buffer) {
        super(buffer);
    }

    public void toChannel(final ByteBuffer buffer, final WritableByteChannel channel) {
        try {
            copied += channel.write(buffer);
        }
        catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
