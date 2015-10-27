package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.io.IOException;

public class CopyData extends Response {

    private int copied = 0;
    
    public CopyData(final ByteBuffer buffer) {
        super(buffer);
    }

    public int getRemaining() {
        return size - copied;
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
