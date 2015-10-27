package db.postgresql.async.types;

import db.postgresql.async.pginfo.PgType;
import static db.postgresql.async.pginfo.PgType.builder;
import static db.postgresql.async.types.UdtHashing.*;

public class Box implements Udt {

    public static final PgType PGTYPE = builder().name("box").oid(603).arrayId(1020).delimiter(';').build();
    
    private final Point upperRight;
    private final Point lowerLeft;

    public Point getUpperRight() { return upperRight; }
    public Point getLowerLeft() { return lowerLeft; }
    public String getName() { return PGTYPE.getName(); }
    
    public Box(final UdtInput input) {
        this(input.read(Point.class), input.read(Point.class));
    }

    public Box(final Point upperRight, final Point lowerLeft) {
        this.upperRight = upperRight;
        this.lowerLeft = lowerLeft;
    }

    public void write(final UdtOutput output) {
        output.writeUdt(upperRight);
        output.writeUdt(lowerLeft);
    }

    @Override
    public String toString() {
        return String.format("%s,%s", upperRight.toString(), lowerLeft.toString());
    }
    
    @Override
    public boolean equals(Object o) {
        if(!(o instanceof Box)) {
            return false;
        }

        Box rhs = (Box) o;
        return upperRight.equals(rhs.upperRight) && lowerLeft.equals(rhs.lowerLeft);
    }

    @Override
    public int hashCode() {
        return hash(hash(START, upperRight), lowerLeft);
    }
}
