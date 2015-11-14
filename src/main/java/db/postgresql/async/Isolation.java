package db.postgresql.async;

public enum Isolation {
    READ_COMMITTED("READ COMMITTED"),
    REPEATABLE_READ("REPEATABLE READ"),
    SERIALIZABLE("SERIALIZABLE");

    private Isolation(final String text) {
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
