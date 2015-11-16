package db.postgresql.async.messages;

import java.util.function.Function;
import java.nio.ByteBuffer;

public enum BackEnd {

    Authentication(Authentication::new),
    BackendKeyData(KeyData::new),
    BindComplete(Response::new),
    CloseComplete(Response::new),
    CommandComplete(CommandComplete::new),
    CopyData(CopyData::new),
    CopyDone(Response::new),
    CopyInResponse(CopyResponse::new),
    CopyOutResponse(CopyResponse::new),
    CopyBothResponse(CopyResponse::new),
    DataRow(DataRow::new),
    EmptyQueryResponse(Response::new),
    ErrorResponse(Notice::new),
    FunctionCallResponse(FunctionCallResponse::new),
    NoData(Response::new),
    NoticeResponse(Notice::new, true),
    NotificationResponse(Notification::new, true),
    ParameterDescription(ParameterDescription::new),
    ParameterStatus(ParameterStatus::new, true),
    ParseComplete(Response::new),
    PortalSuspended(Response::new),
    ReadyForQuery(ReadyForQuery::new),
    RowDescription(RowDescription::new);

    private BackEnd(final Function<ByteBuffer,? extends Response> builder) {
        this(builder, false);
    }

    private BackEnd(final Function<ByteBuffer,? extends Response> builder, final boolean outOfBand) {
        this.builder = builder;
        this.outOfBand = outOfBand;
    }

    public static BackEnd find(final byte lookFor) {
        switch(lookFor) {
        case ((byte) 'R'): return Authentication;
        case ((byte) 'K'): return BackendKeyData;
        case ((byte) '2'): return BindComplete;
        case ((byte) '3'): return CloseComplete;
        case ((byte) 'C'): return CommandComplete;
        case ((byte) 'd'): return CopyData;
        case ((byte) 'c'): return CopyDone;
        case ((byte) 'G'): return CopyInResponse;
        case ((byte) 'H'): return CopyOutResponse;
        case ((byte) 'W'): return CopyBothResponse;
        case ((byte) 'D'): return DataRow;
        case ((byte) 'I'): return EmptyQueryResponse;
        case ((byte) 'E'): return ErrorResponse;
        case ((byte) 'V'): return FunctionCallResponse;
        case ((byte) 'n'): return NoData;
        case ((byte) 'N'): return NoticeResponse;
        case ((byte) 'A'): return NotificationResponse;
        case ((byte) 't'): return ParameterDescription;
        case ((byte) 'S'): return ParameterStatus;
        case ((byte) '1'): return ParseComplete;
        case ((byte) 's'): return PortalSuspended;
        case ((byte) 'Z'): return ReadyForQuery;
        case ((byte) 'T'): return RowDescription;
        default: throw new IllegalStateException(((char) lookFor) + " is not  valid back end message");
        }
    }

    public final Function<ByteBuffer,? extends Response> builder;
    public final boolean outOfBand;
    
    public static int needs(final ByteBuffer buffer) {
        if(buffer.remaining() < 5) {
            return 5 - buffer.remaining();
        }
        
        buffer.mark();
        final char msgHeader = (char) buffer.get();
        final int messageSize = buffer.getInt() - 4;
        final int need = Math.max(messageSize - buffer.remaining(), 0);
        buffer.reset();
        return need;
    }
}
