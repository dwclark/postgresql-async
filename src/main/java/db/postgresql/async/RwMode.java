package db.postgresql.async;

public enum RwMode {

    READ_WRITE("READ WRITE"), READ_ONLY("READ ONLY");

    private RwMode(final String text) {
        this.text = text;
    }

    private final String text;

    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return text;
    }
}
