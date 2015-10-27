package db.postgresql.async.messages;

public enum Format {

    TEXT, BINARY;

    public static Format find(final int i) {
        switch(i) {
        case 0: return TEXT;
        case 1: return BINARY;
        default: throw new IllegalArgumentException("Not a valid format id: " + i);
        }
    }

    public static final Format[] EMPTY_ARRAY = new Format[0];
}
