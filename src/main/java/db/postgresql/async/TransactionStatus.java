package db.postgresql.async;

public enum TransactionStatus {

    IDLE, IN_BLOCK, FAILED;

    public static TransactionStatus from(final byte c) {
        switch(c) {
        case ((byte) 'I'): return IDLE;
        case ((byte) 'T'): return IN_BLOCK;
        case ((byte) 'E'): return FAILED;
        default: throw new IllegalStateException("Not a legal transaction status: " + (char) c);
        }
    }
}
