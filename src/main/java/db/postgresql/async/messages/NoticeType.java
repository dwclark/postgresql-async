package db.postgresql.async.messages;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public enum NoticeType {
    Severity,
    Code,
    Message,
    Detail,
    Hint,
    Position,
    InternalPosition,
    InternalQuery,
    Where,
    SchemaName,
    TableName,
    ColumnName,
    DataTypeName,
    ConstraintName,
    File,
    Line,
    Routine,
    Unknown;

    public static NoticeType find(byte b) {
        switch(b) {
        case ((byte) 'S'): return Severity;
        case ((byte) 'C'): return Code;
        case ((byte) 'M'): return Message;
        case ((byte) 'D'): return Detail;
        case ((byte) 'H'): return Hint;
        case ((byte) 'P'): return Position;
        case ((byte) 'p'): return InternalPosition;
        case ((byte) 'q'): return InternalQuery;
        case ((byte) 'W'): return Where;
        case ((byte) 's'): return SchemaName;
        case ((byte) 't'): return TableName;
        case ((byte) 'c'): return ColumnName;
        case ((byte) 'd'): return DataTypeName;
        case ((byte) 'n'): return ConstraintName;
        case ((byte) 'F'): return File;
        case ((byte) 'L'): return Line;
        case ((byte) 'R'): return Routine;
        case ((byte) ' '):
        default: return Unknown;
        }
    }
}
