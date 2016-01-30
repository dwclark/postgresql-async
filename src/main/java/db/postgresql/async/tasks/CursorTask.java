package db.postgresql.async.tasks;

import db.postgresql.async.Cursor;
import db.postgresql.async.CursorOp;
import db.postgresql.async.Row;
import db.postgresql.async.TaskState;
import db.postgresql.async.messages.DataRow;
import db.postgresql.async.messages.Response;
import db.postgresql.async.messages.ReadyForQuery;
import db.postgresql.async.pginfo.Portal;
import db.postgresql.async.pginfo.Statement;
import db.postgresql.async.serializers.SerializationContext;
import java.util.function.Function;
import java.util.function.BiFunction;

public class CursorTask<T> extends SimpleTask<T> {

    @Override
    public void onDataRow(final DataRow dataRow) {
        try {
            cursor.incrementLastRowsProcessed();
            cursor.incrementTotalRowsProcessed();
            dataRow.with(() -> this.accumulator = processOp.apply(accumulator, dataRow));
        }
        catch(Throwable t) {
            setError(t);
        }
    }

    @Override
    public T getResult() {
        return accumulator;
    }

    @Override
    protected boolean readProcessor(final Response resp) {
        switch(resp.getBackEnd()) {
        case RowDescription:
            cursor.incrementTotalCalls();
            cursor.setLastRowsProcessed(0);
        }

        return super.readProcessor(resp);
    }

    @Override
    public void computeNextState(final int needs) {
        if(needs > 0) {
            nextState = TaskState.needs(needs);
        }
        else if(readyForQuery == null) {
            nextState = TaskState.read();
        }
        else {
            if(op.getAction() == CursorOp.Action.CLOSE) {
                nextState = TaskState.finished();
            }
            else {
                op = findNextOp.apply(cursor);
                nextState = TaskState.start();
            }
        }
    }

    @Override
    public String getSql() {
        return op.toCommand(cursor.getName());
    }

    private CursorOp op;
    private final Cursor cursor;
    private final Function<Cursor,CursorOp> findNextOp;
    private final BiFunction<T,Row,T> processOp;
    
    public CursorTask(final Cursor cursor, final T accumlator,
                      final Function<Cursor,CursorOp> findNextOp,
                      final BiFunction<T,Row,T> processOp) {
        super(null, accumlator);
        this.cursor = cursor;
        this.findNextOp = findNextOp;
        this.processOp = processOp;
        this.op = findNextOp.apply(cursor);
    }
}
