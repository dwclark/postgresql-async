package db.postgresql.async.types;

import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class Inet {

    private static final byte POSTGRES_INET = (byte) 2;
    private static final byte POSTGRES_INET6 = (byte) (POSTGRES_INET + 1);

    private static final short MAX_IP4 = 32;
    private static final short MAX_IP6 = 128;
    
    private final InetAddress address;
    public InetAddress getAddress() { return address; }
    
    private final short netmask;
    public short getNetmask() { return netmask; }

    private final boolean cidr;
    public boolean isCidr() { return cidr; }

    public byte getFamily() {
        return (address instanceof Inet4Address) ? POSTGRES_INET : POSTGRES_INET6;
    }

    public byte getSize() {
        return (byte) ((address instanceof Inet4Address) ? 4 : 16);
    }

    public Inet(final InetAddress address) {
        this(address, (address instanceof Inet4Address) ? MAX_IP4 : MAX_IP6, false);
    }

    public Inet(final InetAddress address, final short netmask) {
        this(address, netmask, false);
    }

    public Inet(final InetAddress address, final short netmask, final boolean cidr) {
        if(!legalNetmask(address, netmask)) {
            throw new IllegalArgumentException(netmask + " is not a legal netmask for " + address);
        }
        
        this.address = address;
        this.netmask = netmask;
        this.cidr = cidr;
    }

    public Inet(final ByteBuffer buffer) {
        final byte family = buffer.get(); //not used
        this.netmask = (short) (0xFF & buffer.get());
        this.cidr = (buffer.get() == 1 ? true : false); //not really used either
        final byte[] bytes = new byte[buffer.get()];
        buffer.get(bytes);
        try {
            this.address = InetAddress.getByAddress(bytes);
        }
        catch(UnknownHostException e) {
            //this should never happen
            throw new RuntimeException(e);
        }
    }

    public void toBuffer(final ByteBuffer buffer) {
        buffer.put(getFamily());
        buffer.put((byte) netmask);
        buffer.put((byte) (cidr ? 1 : 0));
        buffer.put(getSize());
        buffer.put(address.getAddress());
    }

    public static boolean legalNetmask(final InetAddress address, final short netmask) {
        if(netmask < 0) {
            return false;
        }

        if((address instanceof Inet4Address) && netmask > MAX_IP4) {
            return false;
        }

        if((address instanceof Inet6Address) && netmask > MAX_IP6) {
            return false;
        }

        return true;
    }

    public static short defaultNetmask(final InetAddress addr) {
        return (addr instanceof Inet4Address) ? MAX_IP4 : MAX_IP6;
    }

    public static Inet fromString(final String str) {
        return fromString(str, false);
    }

    public static Inet fromString(final String str, final boolean cidr) {
        try {
            final String[] ary = str.split("/");
            final InetAddress addr = InetAddress.getByName(ary[0]);
            if(ary.length == 1) {
                return new Inet(addr, defaultNetmask(addr), cidr);
            }
            else {
                return new Inet(addr, Short.parseShort(ary[1]), cidr);
            }
        }
        catch(UnknownHostException e){
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public String toString() {
        return String.format("%s/%d", address.toString().substring(1), netmask);
    }

    @Override
    public boolean equals(final Object rhs) {
        if(!(rhs instanceof Inet)) {
            return false;
        }

        final Inet obj = (Inet) rhs;
        return (address.equals(obj.address) && netmask == obj.netmask);
    }

    @Override
    public int hashCode() {
        return 947 * (2137 + address.hashCode()) + netmask;
    }
}
