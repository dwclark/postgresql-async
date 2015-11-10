package db.postgresql.async;

public interface CommandStatus {
    String getAction();
    boolean isMutation();
    boolean hasRows();
    int getRows();
    boolean hasOid();
    int getOid();
}
