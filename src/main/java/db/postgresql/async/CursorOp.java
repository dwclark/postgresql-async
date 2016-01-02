package db.postgresql.async;

import java.util.function.Supplier;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

public final class CursorOp {

    public enum Action {
        MOVE("move"), FETCH("fetch"), CLOSE("close");
        
        private Action(final String command) {
            this.command = command;
        }
        
        public final String command;

        @Override
        public String toString() {
            return command;
        }
    }

    @FunctionalInterface
    private interface Clause {
        String command(Action action, int count, String cursorName);
    }

    private static final Map<Direction,Clause> clauses = populateClauses();

    private static Map<Direction,Clause> populateClauses() {
        final Map<Direction,Clause> ret = new HashMap<>();
        ret.put(Direction.NEXT,
                (action, count, name) -> String.format("%s next from \"%s\";", action, name));

        ret.put(Direction.PRIOR,
                (action, count, name) -> String.format("%s prior from \"%s\";", action, name));

        ret.put(Direction.FIRST,
                (action, count, name) -> String.format("%s first from \"%s\";", action, name));

        ret.put(Direction.LAST,
                (action, count, name) -> String.format("%s last from \"%s\";", action, name));

        ret.put(Direction.ABSOLUTE,
                (action, count, name) -> String.format("%s absolute %d from \"%s\";", action, count, name));

        ret.put(Direction.RELATIVE,
                (action, count, name) -> String.format("%s relative %d from \"%s\";", action, count, name));

        ret.put(Direction.ALL,
                (action, count, name) -> String.format("%s all from \"%s\";", action, name));

        ret.put(Direction.FORWARD,
                (action, count, name) -> String.format("%s forward %d from \"%s\";", action, count, name));

        ret.put(Direction.FORWARD_ALL,
                (action, count, name) -> String.format("%s forward all from \"%s\";", action, name));

        ret.put(Direction.BACKWARD,
                (action, count, name) -> String.format("%s backward %d from \"%s\";", action, count, name));

        ret.put(Direction.BACKWARD_ALL,
                (action, count, name) -> String.format("%s backward all from \"%s\";", action, name));

        ret.put(Direction.CLOSE,
                (action, count, name) -> String.format("close \"%s\";", name));

        return Collections.unmodifiableMap(ret);
    }

    private Action action;
    public Action getAction() { return action; }
    
    private Direction direction;
    public Direction getDirection() { return direction; }
    
    private int count;
    public int getCount() { return count; }

    private CursorOp(final Action action, final Direction direction, final int count) {
        this.action = action;
        this.direction = direction;
        this.count = count;
    }

    private static final CursorOp CLOSE = new CursorOp(Action.CLOSE, Direction.CLOSE, 0);
    private static final CursorOp FETCH_ALL = new CursorOp(Action.FETCH, Direction.ALL, 0);
    
    public static CursorOp close() {
        return CLOSE;
    }

    public static CursorOp fetch(final Direction direction, final int count) {
        return new CursorOp(Action.FETCH, direction, count);
    }

    public static CursorOp fetchAll() {
        return FETCH_ALL;
    }

    public static CursorOp move(final Direction direction, final int count) {
        return new CursorOp(Action.MOVE, direction, count);
    }

    public String toCommand(final String name) {
        return clauses.get(direction).command(action, count, name);
    }
    
}
