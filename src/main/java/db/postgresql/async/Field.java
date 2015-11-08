package db.postgresql.async;

public class Field {

    private final int index;
    private final String name;
    private final Class type;

    public int getIndex() { return index; }
    public String getName() { return name; }
    public Class getType() { return type; }

    public Field(final int index, final String name, final Class type) {
        this.index = index;
        this.name = name;
        this.type = type;
    }
}
