package db.postgresql.async.messages;

import java.nio.ByteBuffer;

@FunctionalInterface
interface Header {
    void write(ByteBuffer buffer, int size);
}
