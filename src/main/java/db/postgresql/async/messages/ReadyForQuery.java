package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class ReadyForQuery extends Response {

    public final TransactionStatus status;
    
    public ReadyForQuery(final ByteBuffer buffer) {
        super(buffer);
        this.status = TransactionStatus.from(buffer.get());
    }
}

