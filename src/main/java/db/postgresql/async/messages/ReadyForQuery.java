package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import db.postgresql.async.TransactionStatus;

public class ReadyForQuery extends Response {

    private final TransactionStatus status;
    public TransactionStatus getStatus() { return status; }
    
    public ReadyForQuery(final ByteBuffer buffer) {
        super(buffer);
        this.status = TransactionStatus.from(buffer.get());
    }
}

