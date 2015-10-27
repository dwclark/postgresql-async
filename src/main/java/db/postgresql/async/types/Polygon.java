package db.postgresql.async.types;

import db.postgresql.async.pginfo.PgType;
import static db.postgresql.async.pginfo.PgType.builder;
import static db.postgresql.async.types.UdtHashing.*;
import java.util.List;

public class Polygon extends Path {

    public static final PgType PGTYPE = builder().name("polygon").oid(604).arrayId(1027).build();
    
    public String getName() { return PGTYPE.getName(); }
    
    public Polygon(final UdtInput input) {
        super(input);
    }

    public Polygon(final List<Point> points) {
        super(points, false);
    }

    @Override
    public boolean equals(Object o) {
        if(o == null) {
            return false;
        }
        
        if(!o.getClass().equals(Polygon.class)) {
            return false;
        }
        
        Polygon rhs = (Polygon) o;
        return pointsEqual(rhs.getPoints());
    }
}
