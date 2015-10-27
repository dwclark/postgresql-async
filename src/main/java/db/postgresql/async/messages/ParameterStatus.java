package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class ParameterStatus extends Response {

    private final String name;
    public String getName() { return name; }
    
    private final String value;
    public String getValue() { return value; }
    
    public ParameterStatus(final ByteBuffer buffer) {
        super(buffer);
        this.name = ascii(buffer);
        this.value = ascii(buffer);
    }
}
