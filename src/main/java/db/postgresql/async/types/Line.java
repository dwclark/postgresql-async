package db.postgresql.async.types;

import static db.postgresql.async.types.Hashing.*;
import java.nio.ByteBuffer;

public class Line {

    private final double a;
    private final double b;
    private final double c;

    public double getA() { return a; }
    public double getB() { return b; }
    public double getC() { return c; }

    public Line(final ByteBuffer buffer) {
        this(buffer.getDouble(), buffer.getDouble(), buffer.getDouble());
    }

    public Line(final double a, final double b, final double c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public void toBuffer(final ByteBuffer buffer) {
        buffer.putDouble(a).putDouble(b).putDouble(c);
    }

    public static Line read(final int size, final ByteBuffer buffer, final int oid) {
        return new Line(buffer);
    }

    public static void write(final ByteBuffer buffer, final Object o) {
        ((Line) o).toBuffer(buffer);
    }
    
    @Override
    public String toString() {
        return String.format("{%f,%f,%f}", a, b, c);
    }

    @Override
    public boolean equals(final Object rhs) {
        return (rhs instanceof Line) ? equals((Line) rhs) : false;
    }

    public boolean equals(final Line rhs) {
        return ((a == rhs.a) && (b == rhs.b) && (c == rhs.c));
    }

    @Override
    public int hashCode() {
        return hash(hash(hash(START, a), b), c);
    }
}
