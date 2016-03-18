package db.postgresql.async;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import db.postgresql.async.messages.RowDescription;
import db.postgresql.async.messages.FieldDescriptor;

public interface Field {

    boolean isStreaming();
    FieldDescriptor getFieldDescriptor();
    int getIndex();

    void stream(ByteBuffer buffer);
    void stream(CharBuffer buffer);
    
    Object asObject();
    String asString();
    boolean asBoolean();
    double asDouble();
    float asFloat();
    int asInt();
    long asLong();
    short asShort();
    Object asArray(Class elementType);
}
