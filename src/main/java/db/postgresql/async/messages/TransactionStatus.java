package db.postgresql.async.messages;

public enum TransactionStatus {

    IDLE, IN_BLOCK, FAILED;

    public static TransactionStatus from(final byte val) {
        final char c = (char) val;
        switch(c) {
        case 'I': return IDLE;
        case 'T': return IN_BLOCK;
        case 'E': return FAILED;
        default: throw new IllegalStateException("Not a legal transaction status: " + c);
        }
    }
}
