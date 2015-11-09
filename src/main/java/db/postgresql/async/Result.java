package db.postgresql.async;

public class Result {
    
    private final CommandStatus commandStatus;
    private final TransactionStatus transactionStatus;
    private final Object output;

    public CommandStatus getCommandStatus() {
        return commandStatus;
    }

    public TransactionStatus getTransactionStatus() {
        return transactionStatus;
    }

    public Object getOutput() {
        return output;
    }

    public Result(final CommandStatus commandStatus,
                  final TransactionStatus transactionStatus,
                  final Object output) {
        this.commandStatus = commandStatus;
        this.transactionStatus = transactionStatus;
        this.output = output;
    }
}
