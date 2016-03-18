package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import static java.nio.charset.StandardCharsets.US_ASCII;
import db.postgresql.async.tasks.RowMode;
import db.postgresql.async.Field;
import java.util.function.BiFunction;

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

    protected Response() {
        this.backEnd = null;
        this.size = 0;
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

    public static Response rowMode(final ByteBuffer buffer) {
        if(!BackEnd.hasHeader(buffer)) {
            return new IncompleteResponse();
        }
        else if(BackEnd.needs(buffer) > 0) {
            return new IncompleteResponse(buffer);
        }
        else {
            final int pos = buffer.position();
            return BackEnd.find(buffer.get(pos)).builder.apply(buffer);
        }
    }

    public static <T> Response fieldMode(final ByteBuffer buffer, final T accumulator,
                                         final BiFunction<T,Field,T> func) {
        if(!BackEnd.hasHeader(buffer)) {
            return new IncompleteResponse();
        }
        
        final BackEnd be = BackEnd.find(buffer.get(buffer.position()));
        if(be == BackEnd.DataRow) {
            return new StreamingRow<>(buffer, accumulator, func);
        }
        else {
            return rowMode(buffer);
        }
    }
}
