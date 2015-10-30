package db.postgresql.async.messages;

import java.nio.ByteBuffer;

public enum FrontEnd {

    Bind((buffer, size) -> {
            buffer.put((byte) 'B');
            buffer.putInt(size + 4); }),

    CancelRequest((buffer, size) -> {
            buffer.putInt(16);
            buffer.putInt(80_877_102); }),

    Close((buffer, size) -> {
            buffer.put((byte) 'C');
            buffer.putInt(size + 4); }),

    CopyData((buffer, size) -> {
            buffer.put((byte) 'd');
            buffer.putInt(size + 4); }),

    CopyDone((buffer, size) -> {
            buffer.put((byte) 'c');
            buffer.putInt(4); }),

    CopyFail((buffer, size) -> {
            buffer.put((byte) 'f');
            buffer.putInt(size + 4); }),

    Describe((buffer, size) -> {
            buffer.put((byte) 'D');
            buffer.putInt(size + 4); }),

    Execute((buffer, size) -> {
            buffer.put((byte) 'E');
            buffer.putInt(size + 4); }),

    Flush((buffer, size) -> {
            buffer.put((byte) 'H');
            buffer.putInt(4); }),

    FunctionCall((buffer, size) -> {
            buffer.put((byte) 'F');
            buffer.putInt(size + 4); }),

    Parse((buffer, size) -> {
            buffer.put((byte) 'P');
            buffer.putInt(size + 4); }),

    Password((buffer, size) -> {
            buffer.put((byte) 'p');
            buffer.putInt(size + 4); }),

    Query((buffer, size) -> {
            buffer.put((byte) 'Q');
            buffer.putInt(size + 4); }),

    SSLRequest((buffer, size) -> {
            buffer.putInt(8);
            buffer.putInt(80_877_103); }),

    StartupMessage((buffer, size) -> {
            buffer.putInt(size);
            buffer.putInt(196_608); }),

    Sync((buffer, size) -> {
            buffer.put((byte) 'S');
            buffer.putInt(4); }),

    Terminate((buffer, size) -> {
            buffer.put((byte) 'X');
            buffer.putInt(4); }),

    None((buffer, size) -> { });

    private FrontEnd(final Header header) {
        this.header = header;
    }

    public final Header header;
}
