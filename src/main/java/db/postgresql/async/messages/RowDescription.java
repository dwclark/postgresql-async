package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Arrays;
public class RowDescription extends Response {

    private final FieldDescriptor[] fields;

    public FieldDescriptor field(final int i) {
        return fields[i];
    }

    private String[] names;

    public String[] getNames() {
        if(names == null) {
            String[] tmp = new String[fields.length];
            for(int i = 0; i < fields.length; ++i) {
                tmp[i] = fields[i].getName();
            }

            names = tmp;
        }

        return names;
    }

    public int length() { return fields.length; }

    public int indexOf(final String name) {
        for(int i = 0; i < fields.length; ++i) {
            if(fields[i].getName().equals(name)) {
                return i;
            }
        }

        throw new IllegalArgumentException(name + " is not a valid field name");
    }

    public Iterator<FieldDescriptor> iterator() {
        return Arrays.asList(fields).iterator();
    }
    
    public RowDescription(final ByteBuffer buffer) {
        super(buffer);
        fields = new FieldDescriptor[0xFFFF & buffer.getShort()];
        for(int i = 0; i < fields.length; ++i) {
            fields[i] = new FieldDescriptor(buffer);
        }
    }
}
