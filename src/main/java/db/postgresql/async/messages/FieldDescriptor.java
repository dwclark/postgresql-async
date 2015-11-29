package db.postgresql.async.messages;

import java.nio.ByteBuffer;
import static db.postgresql.async.messages.Response.ascii;

public class FieldDescriptor {
    private final String name;
    public String getName() { return name; }
    
    private final int tableOid;
    public int getTableOid() { return tableOid; }
    
    private final short columnOid;
    public int getColumnOid() { return columnOid; }
    
    private final int typeOid;
    public int getTypeOid() { return typeOid; }
    
    private final short typeSize;
    public short getTypeSize() { return typeSize; }
    
    private final int typeModifier;
    public int getTypeModifier() { return typeModifier; }
    
    private final Format format;
    public Format getFormat() { return format; }

    private FieldDescriptor(final String name, final int tableOid, final short columnOid,
                            final int typeOid, final short typeSize, final int typeModifier, final Format format) {
        this.name = name;
        this.tableOid = tableOid;
        this.columnOid = columnOid;
        this.typeOid = typeOid;
        this.typeSize = typeSize;
        this.typeModifier = typeModifier;
        this.format = format;
    }

    public FieldDescriptor(final ByteBuffer buffer) {
        this(ascii(buffer), buffer.getInt(), buffer.getShort(),
             buffer.getInt(), buffer.getShort(), buffer.getInt(),
             Format.find(buffer.getShort() & 0xFFFF));
    }

    public FieldDescriptor toBinary() {
        return new FieldDescriptor(name, tableOid, columnOid,
                                   typeOid, typeSize, typeModifier, Format.BINARY);
    }

    @Override
    public int hashCode() {
        return (name.hashCode() + tableOid + columnOid + typeOid + typeSize + typeModifier + format.ordinal());
    }

    @Override
    public boolean equals(Object rhs) {
        if(!(rhs instanceof FieldDescriptor)) {
            return false;
        }

        FieldDescriptor fd = (FieldDescriptor) rhs;
        return (name.equals(fd.name) &&
                (tableOid == fd.tableOid) &&
                (columnOid == fd.columnOid) &&
                (typeOid == fd.typeOid) &&
                (typeSize == fd.typeSize) &&
                (typeModifier == fd.typeModifier) &&
                (format == fd.format));
    }
}
