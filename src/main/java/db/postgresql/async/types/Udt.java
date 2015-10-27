package db.postgresql.async.types;

public interface Udt {
    String getName();
    void write(UdtOutput output);

    default char getLeftDelimiter() {
        return '(';
    }

    default char getRightDelimiter() {
        return ')';
    }
}
