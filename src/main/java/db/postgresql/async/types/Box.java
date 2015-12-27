package db.postgresql.async.types;

import static db.postgresql.async.types.Hashing.*;
import java.nio.ByteBuffer;

public class Box {
    
    private final Point upperRight;
    private final Point lowerLeft;

    public Point getUpperRight() { return upperRight; }
    public Point getLowerLeft() { return lowerLeft; }
    
    public Box(final ByteBuffer buffer) {
        this(new Point(buffer), new Point(buffer));
    }

    public Box(final Point upperRight, final Point lowerLeft) {
        this.upperRight = upperRight;
        this.lowerLeft = lowerLeft;
    }

    public void toBuffer(final ByteBuffer buffer) {
        upperRight.toBuffer(buffer);
        lowerLeft.toBuffer(buffer);
    }
    
    @Override
    public String toString() {
        return String.format("%s,%s", upperRight.toString(), lowerLeft.toString());
    }
    
    @Override
    public boolean equals(final Object rhs) {
        return (rhs instanceof Box) ? equals((Box) rhs) : false;
    }

    public boolean equals(final Box rhs) {
        return upperRight.equals(rhs.upperRight) && lowerLeft.equals(rhs.lowerLeft);
    }

    @Override
    public int hashCode() {
        return hash(hash(START, upperRight), lowerLeft);
    }
}
