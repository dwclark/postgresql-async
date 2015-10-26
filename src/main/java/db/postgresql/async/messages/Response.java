package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class Response {

    public BackEnd getBackEnd() {
        return backEnd;
    }

    private final BackEnd backEnd;

    public int getSize() {
        return size;
    }
    
    private final int size;

    public Response(final ByteBuffer buffer) {
        this.backEnd = BackEnd.find(buffer.get());
        this.size = buffer.getInt();
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
        return new String(bytes, ASCII);
    }

    public static final byte NULL = (byte) 0;
    public static final Charset ASCII = Charset.forName("US-ASCII");
}
