package db.postgresql.async.types;

import static db.postgresql.async.types.Hashing.*;
import java.util.Arrays;
import java.nio.ByteBuffer;

public class Polygon {

    final Point[] points;
    
    public Polygon(final ByteBuffer buffer) {
        this(Path.points(buffer));
    }

    public Polygon(final Point[] points) {
        this.points = points;
    }

    public void toBuffer(final ByteBuffer buffer) {
        Path.toBuffer(buffer, points);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(points);
    }

    @Override
    public boolean equals(final Object rhs) {
        return (rhs instanceof Polygon) ? equals((Polygon) rhs) : false;
    }

    public boolean equals(final Polygon rhs) {
        return Arrays.equals(points, rhs.points);
    }

    @Override
    public String toString() {
        return Path.toString(points);
    }
}
