package db.postgresql.async.serializers;

import java.nio.ByteBuffer;

public class Primitives {

    public static boolean isNull(final ByteBuffer buffer) {
        return buffer.getInt() == -1;
    }
    
    public static boolean readBoolean(final ByteBuffer buffer) {
        if(isNull(buffer)) {
            return false;
        }
        else {
            return buffer.get() == 1 ? true : false;
        }
    }

    public static void writeBoolean(final ByteBuffer buffer, final boolean val) {
        buffer.putInt(1).put((byte) (val ? 1 : 0));
    }

    public static short readShort(final ByteBuffer buffer) {
        return isNull(buffer) ? (short) 0 : buffer.getShort();
    }

    public static void writeShort(final ByteBuffer buffer, final short val) {
        buffer.putInt(2).putShort(val);
    }

    public static int readInt(final ByteBuffer buffer) {
        return isNull(buffer) ? 0 : buffer.getInt();
    }

    public static void writeInt(final ByteBuffer buffer, final int val) {
        buffer.putInt(4).putInt(val);
    }

    public static long readLong(final ByteBuffer buffer) {
        return isNull(buffer) ? 0L : buffer.getLong();
    }

    public static void writeLong(final ByteBuffer buffer, final long val) {
        buffer.putInt(8).putLong(val);
    }

    public static float readFloat(final ByteBuffer buffer) {
        return isNull(buffer) ? 0f : buffer.getFloat();
    }

    public static void writeFloat(final ByteBuffer buffer, final float val) {
        buffer.putInt(4).putFloat(val);
    }

    public static double readDouble(final ByteBuffer buffer) {
        return isNull(buffer) ? 0d : buffer.getDouble();
    }

    public static void writeDouble(final ByteBuffer buffer, final double val) {
        buffer.putInt(8).putDouble(val);
    }
}
