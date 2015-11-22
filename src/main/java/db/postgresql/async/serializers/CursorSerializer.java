package db.postgresql.async.serializers;

import java.util.Collections;
import java.util.List;
import db.postgresql.async.Cursor;
import db.postgresql.async.tasks.CursorTask;

public class CursorSerializer extends Serializer<Cursor> {

    private CursorSerializer() { }

    public static final CursorSerializer instance = new CursorSerializer();

    public Class<Cursor> getType() { return Cursor.class; }

    public List<String> getPgNames() {
        return Collections.singletonList("pg_catalog.refcursor");
    }

    public Cursor fromString(final String str) {
        return new CursorTask(SerializationContext.io(), str);
    }

    public String toString(final Cursor cursor) {
        throw new UnsupportedOperationException("Cursors are not serializable");
    }
}
