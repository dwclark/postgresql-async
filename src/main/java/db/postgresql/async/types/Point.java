package db.postgresql.async.types;

import db.postgresql.async.pginfo.PgType;
import static db.postgresql.async.types.UdtHashing.*;
import java.nio.ByteBuffer;

public class Point {

    private final double x;
    private final double y;

    public double getX() { return x; }
    public double getY() { return y; }
    
    public Point(final ByteBuffer buffer) {
        this(buffer.getDouble(), buffer.getDouble());
    }

    public void toBuffer(final ByteBuffer buffer) {
        buffer.putDouble(x).putDouble(y);
    }

    public Point(final double x, final double y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public String toString() {
        return String.format("(%f,%f)", x, y);
    }

    @Override
    public boolean equals(final Object rhs) {
        return (rhs instanceof Point) ? equals((Point) rhs) : false;
    }

    public boolean equals(final Point rhs) {
        return ((x == rhs.x) && (y == rhs.y));
    }

    @Override
    public int hashCode() {
        return hash(hash(START, x), y);
    }
}
