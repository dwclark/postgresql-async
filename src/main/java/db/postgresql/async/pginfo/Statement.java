package db.postgresql.async.pginfo;

import java.util.concurrent.atomic.AtomicInteger;
import db.postgresql.async.messages.RowDescription;
import db.postgresql.async.messages.ParameterDescription;

public class Statement {

    public static final Statement ANONYMOUS = new Statement();
    
    private static final AtomicInteger counter = new AtomicInteger(0);

    private final RowDescription rowDescription;
    public RowDescription getRowDescription() { return rowDescription; }
    
    private final ParameterDescription parameterDescription;
    public ParameterDescription getParameterDescription() { return parameterDescription; }

    public Statement(final String id,
                     final ParameterDescription parameterDescription,
                     final RowDescription rowDescription) {
        this.id = id;
        this.parameterDescription = parameterDescription;
        this.rowDescription = rowDescription;
    }

    private final String id;
    
    public String getId() {
        return id;
    }

    private Statement() {
        this.rowDescription = RowDescription.SINGLE_BINARY;
        this.parameterDescription = null;
        this.id = "";
    }

    public Portal nextPortal() {
        return new Portal(this);
    }

    public static String nextId() {
        return String.format("_%d", counter.incrementAndGet());
    }
}
