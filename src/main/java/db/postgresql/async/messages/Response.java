package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import static java.nio.charset.StandardCharsets.US_ASCII;

public class Response {

    private final BackEnd backEnd;
    public final BackEnd getBackEnd() { return backEnd; }
    
    private final int size;
    public final int getSize() { return size; }

    public int getNeeds() {
        return 0;
    }

    public boolean isFinished() {
        return true;
    }

    public void networkComplete(final ByteBuffer buffer) {
        //empty
    }

    protected Response(final BackEnd backEnd, final int size) {
        this.backEnd = backEnd;
        this.size = size;
    }

    public Response(final ByteBuffer buffer) {
        this(BackEnd.find(buffer.get()), buffer.getInt() - 4);
    }

    public static String ascii(final ByteBuffer buffer) {
        final int start = buffer.position();
        int pos = start;
        while(buffer.get(pos) != NULL) {
            ++pos;
        }

        final byte[] bytes = new byte[pos - start];
        buffer.get(bytes);
        buffer.get();
        return new String(bytes, US_ASCII);
    }

    public static final byte NULL = (byte) 0;
}
