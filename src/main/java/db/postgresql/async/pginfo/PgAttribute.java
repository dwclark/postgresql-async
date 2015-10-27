package db.postgresql.async.pginfo;

public class PgAttribute implements Comparable<PgAttribute> {

    private final int relId;
    public int getRelId() { return relId; }
    
    private final String name;
    public String getName() { return name; }
    
    private final int typeId;
    public int getTypeId() { return typeId; }
    
    private final int num;
    public int getNum() { return num; }
    
    public PgAttribute(final int relId, final String name, final int typeId, final int num) {
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
}
