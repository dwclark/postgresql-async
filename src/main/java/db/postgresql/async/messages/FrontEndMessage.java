package db.postgresql.async.messages;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.BufferOverflowException;
import java.util.Map;
import java.nio.charset.Charset;
import java.nio.channels.WritableByteChannel;

//Not thread safe
public class FrontEndMessage {

    public ByteBuffer buffer;
    public Charset encoding = Charset.forName("UTF-8");

    public int start(final FrontEnd fe) {
        buffer.mark();
        fe.header.write(buffer, 0);
        return buffer.position();
    }

    public boolean end(final FrontEnd fe, final int messageStart) {
        final int current = buffer.position();
        buffer.reset();
        fe.header.write(buffer, current - messageStart);
        buffer.position(current);
        return true;
    }

    private boolean reset() {
        buffer.reset();
        return false;
    }

    private void putNull() { buffer.put((byte) 0); }

    private void putNullString(final String str) {
        buffer.put(str.getBytes(encoding));
        putNull();
    }

    public boolean startup(final Map<String,String> keysValues) {
        try {
            final int messageStart = start(FrontEnd.StartupMessage);
            for(Map.Entry<String,String> pair : keysValues.entrySet()) {
                putNullString(pair.getKey());
                putNullString(pair.getValue());
            }

            putNull();
            return end(FrontEnd.StartupMessage, messageStart);
        }
        catch(BufferOverflowException ex) {
            return reset();
        }
    }

    public boolean close(final char type, final String name) {
        try {
            final int messageStart = start(FrontEnd.Close);
            buffer.put((byte) type);
            putNullString(name);
            return end(FrontEnd.Close, messageStart);
        }
        catch(BufferOverflowException ex) {
            return reset();
        }
    }

    public boolean closeStatement(final String name) {
        return close('S', name);
    }

    public boolean closePortal(final String name) {
        return close('P', name);
    }

    public boolean copyData(final WritableByteChannel channel, final boolean includeHeader) {
        try {
            int messageStart = 0;
            if(includeHeader) {
                messageStart = start(FrontEnd.CopyData);
            }

            final int written = channel.write(buffer);
            if(written == 0) {
                return reset();
            }

            if(includeHeader) {
                return end(FrontEnd.Close, messageStart);
            }
            else {
                return true;
            }
        }
        catch(BufferOverflowException | IOException ex) {
            return reset();
        }
    }

    public boolean copyDone() {
        try {
            return end(FrontEnd.CopyDone, start(FrontEnd.CopyDone));
        }
        catch(BufferOverflowException ex) {
            return reset();
        }
           
            
    }
}
