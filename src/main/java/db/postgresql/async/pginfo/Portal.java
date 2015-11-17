package db.postgresql.async.pginfo;

import db.postgresql.async.messages.RowDescription;

public class Portal {

    private final RowDescription rowDescription;
    public RowDescription getRowDescription() { return rowDescription; }

    private final String value;
    public String getValue() { return value; }

    public Portal(final String value, final RowDescription rowDescription) {
        this.value = value;
        this.rowDescription = rowDescription;
    }

    public String id(final Statement statement) {
        return String.format("%s_portal", statement.getValue());
    }

    public String id(final Statement statement, final int index) {
        return String.format("%s_portal_%d", statement.getValue(), index);
    }
}
