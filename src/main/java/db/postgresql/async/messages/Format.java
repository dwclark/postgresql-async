package db.postgresql.async.messages;

public enum Format {

    TEXT(0), BINARY(1);

    public static Format find(final int i) {
        switch(i) {
        case 0: return TEXT;
        case 1: return BINARY;
        default: throw new IllegalArgumentException("Not a valid format id: " + i);
        }
    }

    private Format(final int code) {
        this.code = (short) code;
    }

    private final short code;

    public short getCode() {
        return code;
    }

    public static final Format[] EMPTY_ARRAY = new Format[0];
}
