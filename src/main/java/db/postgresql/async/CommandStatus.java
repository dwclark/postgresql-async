package db.postgresql.async;

public interface CommandStatus {
    String getAction();
    boolean isMutation();
    boolean isSelect();
    boolean hasRows();
    int getRows();
    boolean hasOid();
    int getOid();
}
