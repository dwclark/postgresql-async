package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class CopyResponse extends Response {

    private final Format format;
    public Format getFormat() { return format; }
    
    private final Format[] columnFormats;
    public Format[] getColumnFormats() { return columnFormats; }
    
    public CopyResponse(final ByteBuffer buffer) {
        super(buffer);
        format = Format.find(buffer.get() & 0xFF);
        final int length = buffer.getShort() & 0xFFFF;
        if(length > 0) {
            columnFormats = new Format[length];
            for(int i = 0; i < length; ++i) {
                columnFormats[i] = Format.find(buffer.getShort() & 0xFFFF);
            }
        }
        else {
            columnFormats = Format.EMPTY_ARRAY;
        }
    }
}
