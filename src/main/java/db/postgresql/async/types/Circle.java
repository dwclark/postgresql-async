package db.postgresql.async.types;

import static db.postgresql.async.types.UdtHashing.*;
import java.nio.ByteBuffer;

public class Circle {
    
    private final Point center;
    private final double radius;

    public Point getCenter() { return center; }
    public double getRadius() { return radius; }
    
    public Circle(final ByteBuffer buffer) {
        this(new Point(buffer), buffer.getDouble());
    }

    public Circle(final Point center, final double radius) {
        this.center = center;
        this.radius = radius;
    }

    public void toBuffer(final ByteBuffer buffer) {
        center.toBuffer(buffer);
        buffer.putDouble(radius);
    }

    @Override
    public String toString() {
        return String.format("<%s,%f>", center.toString(), radius);
    }

    @Override
    public boolean equals(final Object rhs) {
        return (rhs instanceof Circle) ? equals((Circle) rhs) : false;
    }

    public boolean equals(final Circle rhs) {
        return (center.equals(rhs.center) && radius == rhs.radius);
    }

    @Override
    public int hashCode() {
        return hash(hash(START, center), radius);
    }
}
