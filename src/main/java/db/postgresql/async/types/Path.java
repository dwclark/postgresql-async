package db.postgresql.async.types;

import static db.postgresql.async.types.UdtHashing.*;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.nio.ByteBuffer;

public class Path {

    private final Point[] points;
    private final boolean closed;
    
    public Point[] getPoints() {
        return points;
    }

    public boolean isOpen() { return !closed; }
    public boolean isClosed() { return closed; }

    public static Point[] points(final ByteBuffer buffer) {
        final Point[] ret = new Point[buffer.getInt()];
        for(int i = 0; i < ret.length; ++i) {
            ret[i] = new Point(buffer);
        }

        return ret;
    }

    public static String toString(final Point[] points) {
        return "(" + Arrays.stream(points).map(p -> p.toString()).collect(Collectors.joining(",")) + ")";
    }

    public static void toBuffer(final ByteBuffer buffer, final Point[] points) {
        buffer.putInt(points.length);
        for(int i = 0; i < points.length; ++i) {
            points[i].toBuffer(buffer);
        }
    }
    
    public Path(final ByteBuffer buffer) {
        this(buffer.get() == 1 ? true : false, points(buffer));
    }

    public Path(final boolean closed, final Point[] points) {
        this.closed = closed;
        this.points = points;
    }

    public void toBuffer(final ByteBuffer buffer) {
        buffer.put((byte) (closed ? 1 : 0));
        toBuffer(buffer, points);
    }

    @Override
    public String toString() {
        return (closed ? "closed: " : "open: ") + toString(points);
    }

    @Override
    public int hashCode() {
        return (closed ? 1 : 0) + Arrays.hashCode(points);
    }

    @Override
    public boolean equals(final Object rhs) {
        return (rhs instanceof Path) ? equals((Path) rhs) : false;
    }

    public boolean equals(final Path rhs) {
        return (closed == rhs.closed) && Arrays.equals(this.points, rhs.points);
    }
}
