package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public enum FrontEnd {

    Bind((buf, size) -> { buf.put((byte) 'B'); buf.putInt(size + 4); }),
    CancelRequest((buf, size) -> { buf.putInt(16); buf.putInt(80_877_102); }),
    Close((buf, size) -> { buf.put((byte) 'C'); buf.putInt(size + 4); }),
    CopyData((buf, size) -> { buf.put((byte) 'd'); buf.putInt(size + 4); }),
    CopyDone((buf, size) -> { buf.put((byte) 'c'); buf.putInt(4); }),
    CopyFail((buf, size) -> { buf.put((byte) 'f'); buf.putInt(size + 4); }),
    Describe((buf, size) -> { buf.put((byte) 'D'); buf.putInt(size + 4); }),
    Execute((buf, size) -> { buf.put((byte) 'E'); buf.putInt(size + 4); }),
    Flush((buf, size) -> { buf.put((byte) 'H'); buf.putInt(4); }),
    FunctionCall((buf, size) -> { buf.put((byte) 'F'); buf.putInt(size + 4); }),
    Parse((buf, size) -> { buf.put((byte) 'P'); buf.putInt(size + 4); }),
    Password((buf, size) -> { buf.put((byte) 'p'); buf.putInt(size + 4); }),
    Query((buf, size) -> { buf.put((byte) 'Q'); buf.putInt(size + 4); }),
    SSLRequest((buf, size) -> { buf.putInt(8); buf.putInt(80_877_103); }),
    StartupMessage((buf, size) -> { buf.putInt(size); buf.putInt(196_608); }),
    Sync((buf, size) -> { buf.put((byte) 'S'); buf.putInt(4); }),
    Terminate((buf, size) -> { buf.put((byte) 'X'); buf.putInt(4); }),
    None((buf, size) -> { });

    private FrontEnd(final Header header) {
        this.header = header;
    }

    public final Header header;
}
