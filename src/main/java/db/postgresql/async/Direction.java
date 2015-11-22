package db.postgresql.async;

public enum Direction {

    NEXT((type, count, name) -> String.format("%s next from %s", type, name)),
    PRIOR((type, count, name) -> String.format("%s prior from %s", type, name)),
    FIRST((type, count, name) -> String.format("%s first from %s", type, name)),
    LAST((type, count, name) -> String.format("%s last from %s", type, name)),
    ABSOLUTE((type, count, name) -> String.format("%s absolute %d from %s", type, count, name)),
    RELATIVE((type, count, name) -> String.format("%s relative %d from %s", type, count, name)),
    ALL((type, count, name) -> String.format("%s all from %s", type, name)),
    FORWARD((type, count, name) -> String.format("%s forward %d from %s", type, count, name)),
    FORWARD_ALL((type, count, name) -> String.format("%s forward all from %s", type, name)),
    BACKWARD((type, count, name) -> String.format("%s backward %d from %s", type, count, name)),
    BACKWARD_ALL((type, count, name) -> String.format("%s backward all from %s", type, name));

    @FunctionalInterface
    private interface Clause {
        String command(String type, int count, String cursorName);
    }

    public static final int NONE = Integer.MIN_VALUE;
    
    private Direction(final Clause clause) {
        this.clause = clause;
    }

    public String toCommand(final String type, final int count, final String cursorName) {
        return this.clause.command(type, count, cursorName);
    }

    private final Clause clause;
}
