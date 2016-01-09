package db.postgresql.async.pginfo;

import db.postgresql.async.messages.RowDescription;
import java.util.concurrent.atomic.AtomicInteger;

public class Portal {

    final static AtomicInteger counter = new AtomicInteger(0);
    
    private final Statement statement;
    
    public Statement getStatement() {
        return statement;
    }

    private final String id;
    
    public String getId() {
        return id;
    }

    public Portal(final Statement statement) {
        this.statement = statement;
        this.id = String.format("%s_portal_%d", statement.getId(), counter.getAndIncrement());
    }
}
