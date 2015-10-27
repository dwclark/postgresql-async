package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class ParameterDescription extends Response {

    public final int[] oids;

    public ParameterDescription(final ByteBuffer buffer) {
        super(buffer);
        this.oids = new int[buffer.getShort() & 0xFFFF];
        for(int i = 0; i < oids.length; ++i) {
            oids[i] = buffer.getInt();
        }
    }
}
