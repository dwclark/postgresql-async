package db.postgresql.async.types;

import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;

public class Inet {

    private static final short MAX_IP4 = 32;
    private static final short MAX_IP6 = 128;
    
    private final InetAddress address;
    public InetAddress getAddress() { return address; }
    
    private final short netmask;
    public short getNetmask() { return netmask; }

    public Inet(final InetAddress address) {
        if(address instanceof Inet4Address) {
            this.address = address;
            this.netmask = MAX_IP4;
        }
        else {
            this.address = address;
            this.netmask = MAX_IP6;
        }
    }

    public Inet(final InetAddress address, final short netmask) {
        if(!legalNetmask(address, netmask)) {
            throw new IllegalArgumentException(netmask + " is not a legal netmask for " + address);
        }
        
        this.address = address;
        this.netmask = netmask;
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

    public static Inet fromString(final String str) {
        try {
            if(str.indexOf("/") == -1) {
                return new Inet(InetAddress.getByName(str));
            }
            else {
                final String[] ary = str.split("/");
                return new Inet(InetAddress.getByName(ary[0]), Short.parseShort(ary[1]));
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
        return 947 + (2137 + address.hashCode()) * 947;
    }
}
