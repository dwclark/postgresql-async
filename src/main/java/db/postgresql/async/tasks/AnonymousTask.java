package db.postgresql.async.tasks;

import db.postgresql.async.QueryPart;
import db.postgresql.async.NullOutput;
import db.postgresql.async.Isolation;
import db.postgresql.async.RwMode;
import db.postgresql.async.Row;
import db.postgresql.async.TaskState;
import db.postgresql.async.messages.CommandComplete;
import db.postgresql.async.messages.DataRow;
import db.postgresql.async.messages.Format;
import db.postgresql.async.messages.FrontEndMessage;
import db.postgresql.async.messages.Response;
import db.postgresql.async.messages.ReadyForQuery;
import db.postgresql.async.messages.RowDescription;
import db.postgresql.async.pginfo.Statement;
import db.postgresql.async.serializers.SerializationContext;
import java.nio.ByteBuffer;
import java.util.function.Function;
import java.util.function.BiFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ListIterator;
import java.util.List;

public abstract class AnonymousTask<T> extends SimpleTask<T> {

    final private List<Object> args;
    
    public AnonymousTask(final String sql, final T accumulator, final List<Object> args) {
        super(sql, accumulator);
        this.args = (args == null) ? Collections.emptyList() : args;
    }
    
    @Override
    protected boolean readProcessor(final Response resp) {
        switch(resp.getBackEnd()) {
        case ParseComplete:
            return true;
        case BindComplete:
            return true;
        case ParameterDescription:
            return true;
        case RowDescription:
            SerializationContext.description(((RowDescription) resp).toBinary());
            return true;
        default:
            return super.readProcessor(resp);
        }
    }

    @Override
    public void onStart(final FrontEndMessage fe, final ByteBuffer readBuffer) {
        fe.parse("", getSql(), FrontEndMessage.EMPTY_OIDS);
        fe.bind(Statement.ANONYMOUS, args, Format.BINARY);
        fe.describeStatement("");
        fe.execute(Statement.ANONYMOUS);
        fe.sync();
        nextState = TaskState.write();
    }

    public static class NoOutput extends AnonymousTask<NullOutput> {
        public NoOutput(final String sql, final List<Object> args) {
            super(sql, null, args);
        }

        public NullOutput getResult() {
            return NullOutput.instance;
        }

        public void onDataRow(final DataRow dataRow) {
            throw new UnsupportedOperationException();
        }
    }

    public static class Execute extends AnonymousTask<Integer> {
        public Execute(final String sql, final List<Object> args) {
            super(sql, null, args);
        }

        public Integer getResult() {
            return commandComplete.getRows();
        }

        public void onDataRow(final DataRow dataRow) {
            throw new UnsupportedOperationException();
        }
    }
    
    public static class Query<T> extends AnonymousTask<T> {
        private final BiFunction<T,Row,T> func;
        
        public Query(final String sql, final T accumulator, final List<Object> args,
                     final BiFunction<T, Row, T> func) {
            super(sql, accumulator, args);
            this.func = func;
        }

        public void onDataRow(final DataRow dataRow) {
            dataRow.with(() -> this.accumulator = func.apply(accumulator, dataRow));
        }

        public T getResult() {
            return accumulator;
        }
    }

    public static <T> AnonymousTask<T> query(final String sql, final T accumulator, final List<Object> args,
                                             final BiFunction<T,Row,T> func) {
        return new Query<>(sql, accumulator, args, func);
    }

    public static <T> AnonymousTask<List<T>> query(final String sql, final List<Object> args, final Function<Row,T> func) {
        final BiFunction<List<T>, Row, List<T>> biFunc = (list, row) -> {
            list.add(func.apply(row));
            return list;
        };

        return query(sql, new ArrayList<>(), args, biFunc);
    }

    public static AnonymousTask<Integer> execute(final String sql, final List<Object> args) {
        return new Execute(sql, args);
    }

    public static AnonymousTask<NullOutput> noOutput(final String sql, final List<Object> args) {
        return new NoOutput(sql, args);
    }

    public static AnonymousTask<NullOutput> begin(final Isolation isolation, final RwMode mode, final boolean deferrable) {
        final String sql = String.format("BEGIN ISOLATION LEVEL %s %s %s;", isolation, mode,
                                         deferrable ? "DEFERRABLE" : "NOT DEFERRABLE");
        return noOutput(sql, Collections.emptyList());
    }

    public static AnonymousTask<NullOutput> commit() {
        return noOutput("commit;", Collections.emptyList());
    }

    public static AnonymousTask<NullOutput> rollback() {
        return noOutput("rollback;", Collections.emptyList());
    }
}
