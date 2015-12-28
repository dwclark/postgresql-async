package db.postgresql.async.types;

import java.nio.ByteBuffer;
import static db.postgresql.async.serializers.SerializationContext.*;

public class Jsonb {

    private static final int DEFAULT_VERSION = 1;
    
    private final int version;
    private final String json;

    public int getVersion() { return version; }
    public String getJson() { return json; }
    
    public Jsonb(final int version, final String json) {
        this.version = version;
        this.json = json;
    }

    public Jsonb(final String json) {
        this(DEFAULT_VERSION, json);
    }

    public Jsonb(final int size, final ByteBuffer buffer) {
        this.version = (int) buffer.get();
        this.json = bufferToString(size - 1, buffer);
    }

    public void toBuffer(final ByteBuffer buffer) {
        buffer.put((byte) version);
        stringToBuffer(buffer, json);
    }

    public static Jsonb read(final int size, final ByteBuffer buffer, final int oid) {
        return new Jsonb(size, buffer);
    }

    public static void write(final ByteBuffer buffer, final Object o) {
        ((Jsonb) o).toBuffer(buffer);
    }

    @Override
    public int hashCode() {
        return 929 * (397 + version) + json.hashCode();
    }

    @Override
    public boolean equals(final Object rhs) {
        return (rhs instanceof Jsonb) ? equals((Jsonb) rhs) : false;
    }

    public boolean equals(final Jsonb rhs) {
        return version == rhs.version && json.equals(rhs.json);
    }

    @Override
    public String toString() {
        return json;
    }
}
