package db.postgresql.async.types;

import static db.postgresql.async.types.Hashing.*;
import java.nio.ByteBuffer;

public class LineSegment {

    private final Point left;
    private final Point right;

    public Point getLeft() { return left; }
    public Point getRight() { return right; }
    
    public LineSegment(final ByteBuffer buffer) {
        this(new Point(buffer), new Point(buffer));
    }

    public LineSegment(final Point left, final Point right) {
        this.left = left;
        this.right = right;
    }

    public void toBuffer(final ByteBuffer buffer) {
        left.toBuffer(buffer);
        right.toBuffer(buffer);
    }

    @Override
    public String toString() {
        return String.format("(%s,%s)", left.toString(), right.toString());
    }
    
    @Override
    public boolean equals(final Object rhs) {
        return (rhs instanceof LineSegment) ? equals((LineSegment) rhs) : false;
    }

    public boolean equals(final LineSegment rhs) {
        return left.equals(rhs.left) && right.equals(rhs.right);
    }

    @Override
    public int hashCode() {
        return hash(hash(START, left), right);
    }
}
