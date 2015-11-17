package db.postgresql.async.pginfo;

import java.util.concurrent.atomic.AtomicInteger;
import db.postgresql.async.messages.RowDescription;
import db.postgresql.async.messages.ParameterDescription;

public class Statement extends Portal {

    private static final AtomicInteger counter = new AtomicInteger(0);
    
    private final ParameterDescription parameterDescription;
    public ParameterDescription getParameterDescription() { return parameterDescription; }

    private final String portalValue;
    public String getPortalValue() { return portalValue; }

    public Statement(final String id,
                     final ParameterDescription parameterDescription,
                     final RowDescription rowDescription) {
        super(id, rowDescription);
        this.parameterDescription = parameterDescription;
        this.portalValue = String.format("%s_portal", id);
    }

    public static String nextId() {
        return String.format("_%d", counter.incrementAndGet());
    }
}
