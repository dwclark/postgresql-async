package db.postgresql.async.types;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class MacAddr {

    private static final int SIZE = 6;
    private static final Pattern REGEX =
        Pattern.compile("([0-9a-f]{2})[:|\\-]([0-9a-f]{2})[:|\\-]([0-9a-f]{2})[:|\\-]" +
                        "([0-9a-f]{2})[:|\\-]([0-9a-f]{2})[:|\\-]([0-9a-f]{2})");
        
    private final byte[] bytes = new byte[SIZE];

    private MacAddr() {}
    
    public MacAddr(final byte[] mac) {
        for(int i = 0; i < SIZE; ++i) {
            bytes[i] = mac[i];
        }
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
        if(!(rhs instanceof MacAddr)) {
            return false;
        }

        final MacAddr addr = (MacAddr) rhs;
        for(int i = 0; i < SIZE; ++i) {
            if(bytes[i] != addr.bytes[i]) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hash = 2137;
        for(byte b : bytes) {
            hash = hash + (0xFF & b) * 947;
        }

        return hash;
    }
}
