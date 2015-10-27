package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class ParameterStatus extends Response {

    public final String name;
    public final String value;
    
    public ParameterStatus(final ByteBuffer buffer) {
        super(buffer);
        this.name = ascii(buffer);
        this.value = ascii(buffer);
    }
}
