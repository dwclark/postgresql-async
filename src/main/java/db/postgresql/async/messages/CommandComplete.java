package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class CommandComplete extends Response {

    enum Action { INSERT, DELETE, UPDATE, SELECT, MOVE, FETCH, COPY; }

    private final Action action;
    public Action getAction() { return action; }
    
    private final int rows;
    public int getRows() { return rows; }
    
    private final int oid;
    public int getOid() { return oid; }
    
    public CommandComplete(final ByteBuffer buffer) {
        super(buffer);
        final String[] ary = ascii(buffer).split(" ");
        if(ary.length == 2) {
            this.action = Action.valueOf(ary[0]);
            this.rows = 0;
            this.oid = Integer.parseInt(ary[1]);
        }
        else {
            this.action = Action.valueOf(ary[0]);
            this.rows = Integer.parseInt(ary[1]);
            this.oid = Integer.parseInt(ary[2]);
        }
    }
}