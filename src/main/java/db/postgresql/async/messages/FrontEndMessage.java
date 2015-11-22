package db.postgresql.async.messages;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.BufferOverflowException;
import java.util.List;
import java.util.Map;
import java.nio.charset.Charset;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import static java.nio.ByteBuffer.wrap;
import db.postgresql.async.pginfo.Statement;
import db.postgresql.async.pginfo.Registry;
import db.postgresql.async.serializers.SerializationContext;
import db.postgresql.async.serializers.Serializer;

//Not thread safe
public class FrontEndMessage {

    private static final Guarded TRUE = () -> true;

    public final Charset encoding;
    public ByteBuffer buffer;
    
    public FrontEndMessage(final Charset encoding) {
        this.encoding = (encoding == null) ? Charset.forName("UTF-8") : encoding;
    }
    
    public int start(final FrontEnd fe) {
        buffer.mark();
        fe.header.write(buffer, 0);
        return buffer.position();
    }

    public boolean end(final FrontEnd fe, final int messageStart) {
        final int current = buffer.position();
        buffer.reset();
        fe.header.write(buffer, current - messageStart);
        buffer.position(current);
        return true;
    }

    private boolean reset() {
        buffer.reset();
        return false;
    }

    private void putNull() { buffer.put((byte) 0); }

    private void putNullString(final String str) {
        buffer.put(str.getBytes(encoding));
        putNull();
    }

    private interface Guarded {
        boolean execute();
    }

    private boolean guard(final FrontEnd frontEnd, final Guarded guarded) {
        try {
            final int messageStart = start(frontEnd);
            return guarded.execute() ? end(frontEnd, messageStart) : reset();
        }
        catch(BufferOverflowException ex) {
            return reset();
        }
    }

    public boolean startup(final Map<String,String> keysValues) {
        return guard(FrontEnd.StartupMessage, () -> {
                for(Map.Entry<String,String> pair : keysValues.entrySet()) {
                    putNullString(pair.getKey());
                    putNullString(pair.getValue());
                }
                
                putNull();
                return true; });
    }

    public static final Object[] EMPTY_ARGS = new Object[0];

    public boolean bindExecuteSync(final Statement statement, final List<Object> args) {
        final int startAt = buffer.position();
        final boolean success = bind(statement, args) && execute(statement) && sync();
        if(!success) {
            buffer.position(startAt);
        }
        
        return success;
    }
    
    public boolean bind(final Statement statement, final List<Object> args) {
        return guard(FrontEnd.Bind, () -> {
                putNullString(statement.getPortalValue());
                putNullString(statement.getValue());
                buffer.putShort((short) args.size());

                for(int i = 0; i < args.size(); ++i) {
                    buffer.putShort(Format.TEXT.getCode());
                }

                buffer.putShort((short) args.size());
                for(int i = 0; i < args.size(); ++i) {
                    final Object arg = args.get(i);
                    if(arg == null) {
                        buffer.putInt(-1);
                    }
                    else {
                        final int sizeAt = buffer.position();
                        buffer.putInt(0);
                        writeArg(arg);
                        final int lastAt = buffer.position();
                        buffer.position(sizeAt);
                        buffer.putInt((lastAt - sizeAt) - 4);
                        buffer.position(lastAt);
                    }
                }

                buffer.putShort((short) 0);
                return true; });
    }
    
    public boolean execute(final Statement statement) {
        return execute(statement, 0);
    }
    
    public boolean execute(final Statement statement, final int maxRows) {
        return guard(FrontEnd.Execute, () -> {
                putNullString(statement.getPortalValue());
                buffer.putInt(maxRows);
                return true; });
    }


    @SuppressWarnings("unchecked")
    private void writeArg(final Object arg) {
        final Serializer s = SerializationContext.registry().serializer(arg.getClass());
        s.write(buffer, arg);
    }

    public boolean cancel(final int pid, final int secretKey) {
        return guard(FrontEnd.CancelRequest, () -> { buffer.putInt(pid); buffer.putInt(secretKey); return true; });
    }

    public boolean close(final char type, final String name) {
        return guard(FrontEnd.Close, () -> {
                buffer.put((byte) type);
                putNullString(name);
                return true; });
    }

    public boolean closeStatement(final String name) {
        return close('S', name);
    }

    public boolean closePortal(final String name) {
        return close('P', name);
    }

    public boolean copyData(final WritableByteChannel channel, final boolean includeHeader) {
        return guard(includeHeader ? FrontEnd.CopyData : FrontEnd.None, () -> {
                try {
                    return (channel.write(buffer) > 0);
                }
                catch(IOException ex) {
                    return false;
                } });
    }

    public boolean copyDone() {
        return guard(FrontEnd.CopyDone, TRUE);
    }

    public boolean copyFail(final String message) {
        return guard(FrontEnd.CopyFail, () -> { putNullString(message); return true; });
    }

    public boolean describe(final char type, final String name) {
        return guard(FrontEnd.Describe, () -> { buffer.put((byte) type); putNullString(name); return true; });
    }

    public boolean describeStatement(final String name) {
        return describe('S', name);
    }

    public boolean describePortal(final String name) {
        return describe('P', name);
    }

    public boolean flush() {
        return guard(FrontEnd.Flush, TRUE);
    }

    public static final int[] EMPTY_OIDS = new int[0];
    
    public boolean parse(final String name, final String query, int[] oids) {
        return guard(FrontEnd.Parse, () -> {
                putNullString(name);
                putNullString(query);
                buffer.putShort((short) oids.length);
                for(int oid : oids) {
                    buffer.putInt(oid);
                }

                return true; });
    }

    public boolean password(final String password) {
        return password(password.getBytes(encoding));
    }

    public boolean password(final byte[] bytes) {
        return guard(FrontEnd.Password, () -> { buffer.put(bytes); return true; });
    }

    public static String toHex(final byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for(byte b : bytes) {
            sb.append(String.format("%02x", b));
        }

        return sb.toString();
    }
    
    public static String compute(final ByteBuffer first, final ByteBuffer second) {
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.update(first);
            m.update(second);
            return toHex(m.digest());
        }
        catch(NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public String md5Hash(final String user, final String password, final ByteBuffer salt) {
        final ByteBuffer userBytes = wrap(user.getBytes(encoding));
        final ByteBuffer passwordBytes = wrap(password.getBytes(encoding));
        return "md5" + compute(wrap(compute(passwordBytes, userBytes).getBytes(encoding)), salt);
    }

    public boolean md5(final String user, final String password, final ByteBuffer salt) {
        return password(md5Hash(user, password, salt));
    }

    public boolean query(final String str) {
        return guard(FrontEnd.Query, () -> { putNullString(str); return true; });
    }

    public boolean sync() {
        return guard(FrontEnd.Sync, TRUE);
    }

    public boolean terminate() {
        return guard(FrontEnd.Terminate, TRUE);
    }
}
