package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class RowDescription extends Response {

    private final FieldDescriptor[] fields;

    public FieldDescriptor field(final int i) {
        return fields[i];
    }
    
    public RowDescription(final ByteBuffer buffer) {
        super(buffer);
        fields = new FieldDescriptor[0xFFFF & buffer.getShort()];
        for(int i = 0; i < fields.length; ++i) {
            fields[i] = new FieldDescriptor(buffer);
        }
    }
}
