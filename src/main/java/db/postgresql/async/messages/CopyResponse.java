package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class CopyResponse extends Response {

    public final Format format;
    public final Format[] columnFormats;
    
    public CopyResponse(final ByteBuffer buffer) {
        super(buffer);
        format = Format.from(buffer.get() & 0xFF);
        final int length = buffer.getShort() & 0xFFFF;
        if(length > 0) {
            columnFormats = new Format[length];
            for(int i = 0; i < length; ++i) {
                columnFormats[i] = Format.from(buffer.getShort() & 0xFFFF);
            }
        }
        else {
            columnFormats = Format.EMPTY_ARRAY;
        }
    }
}
