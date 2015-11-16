package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import db.postgresql.async.CommandStatus;
import java.util.Arrays;
import java.util.stream.Collectors;

public class CommandComplete extends Response implements CommandStatus {

    final String action;
    final int rows;
    final int oid;
    
    public String getAction() { return action; }
    
    public boolean isMutation() {
        return ("UPDATE".equals(action) ||
                "INSERT".equals(action) ||
                "DELETE".equals(action));
    }

    public boolean isSelect() {
        return "SELECT".equals(action);
    }

    public boolean hasRows() { return rows != -1; }
    public int getRows() { return rows; }

    public boolean hasOid() { return oid != -1; }
    public int getOid() { return oid; }
    
    public CommandComplete(final ByteBuffer buffer) {
        super(buffer);
        final String[] parts = ascii(buffer).split(" ");
        this.rows = extract(parts, parts.length - 1);
        this.oid = (this.rows == -1) ? -1 : extract(parts, parts.length - 2);
        this.action = Arrays.stream(parts, 0, lastActionIndex(parts)).collect(Collectors.joining(" "));
    }

    private final int lastActionIndex(final String[] parts) {
        if(rows == -1 && oid == -1) {
            return parts.length;
        }
        else if(rows != -1 && oid == -1) {
            return parts.length - 1;
        }
        else {
            return parts.length - 2;
        }
    }

    private final int extract(final String[] parts, final int at) {
        if(at < 0) {
            return -1;
        }
        
        try {
            return Integer.parseInt(parts[at]);
        }
        catch(NumberFormatException nfe) {
            return -1;
        }
    }
}
