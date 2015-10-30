package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class DataRow extends Response {

    private final int numberColumns;
    
    public DataRow(final ByteBuffer buffer) {
        super(buffer);
        numberColumns = buffer.getShort() & 0xFFFF;
    }
}
