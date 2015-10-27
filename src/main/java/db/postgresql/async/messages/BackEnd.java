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
    DataRow((buffer) -> { throw new UnsupportedOperationException(); }),
    EmptyQueryResponse(Response::new),
    ErrorResponse(Notice::new),
    FunctionCallResponse(FunctionCallResponse::new),
    NoData(Response::new),
    NoticeResponse(Notice::new),
    NotificationResponse(Notification::new),
    ParameterDescription(ParameterDescription::new),
    ParameterStatus(ParameterStatus::new),
    ParseComplete(Response::new),
    PortalSuspended(Response::new),
    ReadyForQuery(ReadyForQuery::new),
    RowDescription(RowDescription::new);

    private BackEnd(final Function<ByteBuffer,? extends Response> builder) {
        this.builder = builder;
    }

    public static BackEnd find(final byte b) {
        final char lookFor = (char) b;
        switch(lookFor) {
        case 'R': return Authentication;
        case 'K': return BackendKeyData;
        case '2': return BindComplete;
        case '3': return CloseComplete;
        case 'C': return CommandComplete;
        case 'd': return CopyData;
        case 'c': return CopyDone;
        case 'G': return CopyInResponse;
        case 'H': return CopyOutResponse;
        case 'W': return CopyBothResponse;
        case 'D': return DataRow;
        case 'I': return EmptyQueryResponse;
        case 'E': return ErrorResponse;
        case 'V': return FunctionCallResponse;
        case 'n': return NoData;
        case 'N': return NoticeResponse;
        case 'A': return NotificationResponse;
        case 't': return ParameterDescription;
        case 'S': return ParameterStatus;
        case '1': return ParseComplete;
        case 's': return PortalSuspended;
        case 'Z': return ReadyForQuery;
        case 'T': return RowDescription;
        default: throw new IllegalStateException(lookFor + " is not  valid back end message");
        }
    }

    public final Function<ByteBuffer,? extends Response> builder;
}
