package db.postgresql.async.pginfo;

import static db.postgresql.async.types.Hashing.*;

public class PgAttribute implements Comparable<PgAttribute> {

    private final int relId;
    public int getRelId() { return relId; }
    
    private final String name;
    public String getName() { return name; }
    
    private final int typeId;
    public int getTypeId() { return typeId; }
    
    private final short num;
    public short getNum() { return num; }
    
    public PgAttribute(final int relId, final String name, final int typeId, final short num) {
        this.relId = relId;
        this.name = name;
        this.typeId = typeId;
        this.num = num;
    }

    @Override
    public String toString() {
        return new StringBuilder(96)
            .append("PgAttribute(")
            .append("name: ").append(name).append(", ")
            .append("relId: ").append(relId).append( ", ")
            .append("typeId: ").append(typeId).append(", ")
            .append("num: ").append(num).append(")").toString();
    }

    public int compareTo(final PgAttribute rhs) {
        if(relId != rhs.relId) {
            throw new IllegalArgumentException("You can't compare pg attributes with different rel ids");
        }

        return Integer.compare(num, rhs.num);
    }

    public PgType getPgType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
        return hash(hash(hash(hash(START, relId), name), typeId), num);
    }

    @Override
    public boolean equals(final Object rhs) {
        return (rhs instanceof PgAttribute) ? equals((PgAttribute) rhs) : false;
    }

    public boolean equals(final PgAttribute rhs) {
        return ((relId == rhs.relId) &&
                (typeId == rhs.typeId) &&
                (num == rhs.num) &&
                name.equals(rhs.name));
    }
}
