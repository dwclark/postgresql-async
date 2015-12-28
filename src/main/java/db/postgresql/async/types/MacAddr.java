package db.postgresql.async.types;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Arrays;
import java.nio.ByteBuffer;

public class MacAddr {

    private static final int SIZE = 6;
    private static final Pattern REGEX =
        Pattern.compile("([0-9a-f]{2})[:|\\-]([0-9a-f]{2})[:|\\-]([0-9a-f]{2})[:|\\-]" +
                        "([0-9a-f]{2})[:|\\-]([0-9a-f]{2})[:|\\-]([0-9a-f]{2})");
        
    private final byte[] bytes = new byte[SIZE];

    private MacAddr() {}
    
    public MacAddr(final byte[] mac) {
        System.arraycopy(mac, 0, bytes, 0, SIZE);
    }

    public MacAddr(final ByteBuffer buffer) {
        buffer.get(bytes);
    }

    public void toBuffer(final ByteBuffer buffer) {
        buffer.put(bytes);
    }

    public static MacAddr read(final int size, final ByteBuffer buffer, final int oid) {
        return new MacAddr(buffer);
    }

    public static void write(final ByteBuffer buffer, final Object o) {
        ((MacAddr) o).toBuffer(buffer);
    }
    
    public byte[] getBytes() {
        final byte[] ret = new byte[SIZE];
        System.arraycopy(bytes, 0, ret, 0, SIZE);
        return ret;
    }

    public static MacAddr fromString(final String str) {
        final Matcher matcher = REGEX.matcher(str);
        if(matcher.matches()) {
            final MacAddr addr = new MacAddr();
            for(int i = 1; i <= matcher.groupCount(); ++i) {
                addr.bytes[i-1] = (byte) Integer.parseInt(matcher.group(i), 16);
            }

            return addr;
        }
        else {
            throw new IllegalArgumentException("Not a valid format for a mac address");
        }
    }

    @Override
    public String toString() {
        return String.format("%02x-%02x-%02x-%02x-%02x-%02x",
                             0xFF & bytes[0], 0xFF & bytes[1],
                             0xFF & bytes[2], 0xFF & bytes[3],
                             0xFF & bytes[4], 0xFF & bytes[5]);
    }

    @Override
    public boolean equals(final Object rhs) {
        return (rhs instanceof MacAddr) ? equals((MacAddr) rhs) : false;
    }

    public boolean equals(final MacAddr rhs) {
        return Arrays.equals(bytes, rhs.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
