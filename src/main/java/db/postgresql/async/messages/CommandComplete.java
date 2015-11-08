package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import db.postgresql.async.Action;
import db.postgresql.async.CommandStatus;

public class CommandComplete extends Response implements CommandStatus {

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
