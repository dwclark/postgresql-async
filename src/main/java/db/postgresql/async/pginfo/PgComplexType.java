package db.postgresql.async.pginfo;

import java.util.Collections;
import java.util.SortedSet;

public class PgComplexType {

    private final int relId;
    public int getRelId() { return relId; }
    
    private SortedSet<PgAttribute> attributes;
    public SortedSet<PgAttribute> getAttributes() { return attributes; }
    
    private PgComplexType(final int relId, final SortedSet<PgAttribute> attributes) {
        this.relId = relId;
        this.attributes = Collections.unmodifiableSortedSet(attributes);
    }
    
    @Override
    public boolean equals(final Object rhs) {
        if(!(rhs instanceof PgComplexType)) {
            return false;
        }

        final PgComplexType o = (PgComplexType) rhs;
        return relId == o.relId;
    }

    @Override
    public int hashCode() {
        return relId;
    }
}
