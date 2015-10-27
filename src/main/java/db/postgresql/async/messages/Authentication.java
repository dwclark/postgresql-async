package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public class Authentication extends Response {

    public enum Type {
        AuthenticationOk,
        AuthenticationKerberosV5,
        AuthenticationCleartextPassword,
        AuthenticationMD5Password,
        AuthenticationSCMCredential,
        AuthenticationGSS,
        AuthenticationSSPI,
        AuthenticationGSSContinue;

        public static Type find(final int b) {
            switch(b) {
            case 0: return AuthenticationOk;
            case 2: return AuthenticationKerberosV5;
            case 3: return AuthenticationCleartextPassword;
            case 5: return AuthenticationMD5Password;
            case 6: return AuthenticationSCMCredential;
            case 7: return AuthenticationGSS;
            case 8: return AuthenticationGSSContinue;
            case 9: return AuthenticationSSPI;
            default: throw new IllegalStateException(b + " is not a legal authentication type");
            }
        }
    }

    private final Type type;
    public Type getType() { return type; }
    
    private final byte[] salt;
    public byte[] getSalt() { return salt; }

    public Authentication(final ByteBuffer buffer) {
        super(buffer);
        this.type = Type.find(buffer.getInt());
        if(type == Type.AuthenticationMD5Password) {
            byte[] tmp = new byte[4];
            buffer.get(tmp);
            this.salt = tmp;
        }
        else {
            this.salt = new byte[0];
        }
    }
}
