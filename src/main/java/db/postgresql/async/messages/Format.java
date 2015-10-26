package db.postgresql.async.messages;

public enum Format {

    TEXT(0), BINARY(1);

    private Format(int id) {
        this.id = id;
    }

    public final int id;

    public static Format from(final int i) {
        if(i == TEXT.id) {
            return TEXT;
        }
        else if(i == BINARY.id) {
            return BINARY;
        }
        else {
            throw new IllegalArgumentException("Not a valid format id: " + i);
        }
    }

    public static final Format[] EMPTY_ARRAY = new Format[0];
}
