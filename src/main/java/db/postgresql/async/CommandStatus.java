package db.postgresql.async;

public interface CommandStatus {
    Action getAction();
    int getRows();
    int getOid();
}
