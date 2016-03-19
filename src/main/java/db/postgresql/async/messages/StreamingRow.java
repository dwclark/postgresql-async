package db.postgresql.async.messages;

import db.postgresql.async.pginfo.PgType;
import db.postgresql.async.Field;
import static db.postgresql.async.serializers.Primitives.*;
import db.postgresql.async.serializers.SerializationContext;
import java.nio.ByteBuffer;
import java.nio.Buffer;
import java.nio.CharBuffer;
import db.postgresql.async.pginfo.Registry;
import java.util.function.BiFunction;
import java.nio.charset.CharsetDecoder;

public class StreamingRow<T> extends Response implements Field {

    private enum State { NOT_STARTED, INCOMPLETE, FIXED, STREAMING };

    private final RowDescription rowDescription;
    private final Registry registry;
    private ByteBuffer buffer;
    private final BiFunction<T,Field,T> func;
    
    private T accumulator;
    private int index = 0;
    private int callIndex = -1;
    private int soFar = 0;

    private State state = State.NOT_STARTED;
    private int currentSize = 0;
    private int currentSoFar = 0;
    private Buffer fieldBuffer;
    private CharsetDecoder decoder;
    
    public StreamingRow(final ByteBuffer buffer, final T accumulator,
                        final BiFunction<T,Field,T> func) {
        super(buffer);
        this.accumulator = accumulator;
        this.func = func;
        this.registry = SerializationContext.registry();
        this.rowDescription = SerializationContext.description();
    }

    public int getCurrentLeft() {
        return currentSize - currentSoFar;
    }

    public int getIndex() {
        return index;
    }

    private int needs;
    
    @Override
    public int getNeeds() {
        return needs;
    }

    @Override
    public boolean isFinished() {
        return getNeeds() == 0;
    }

    private void notStarted() {
        if(buffer.remaining() < 2) {
            needs = 2 - buffer.remaining();
        }
        else {
            buffer.getShort(); //skip column count
            state = State.INCOMPLETE;
        }
    }

    private void incomplete() {
        if(buffer.remaining() < 4) {
            needs = 4 - buffer.remaining();
        }
        else {
            currentSize = buffer.getInt();
            soFar += 4;
            
            FieldDescriptor desc = rowDescription.field(index);
            if(registry.streamable(desc.getTypeOid())) {
                state = State.STREAMING;
            }
            else {
                state = State.FIXED;
            }
        }
    }

    private void fixed() {
        if(currentSize > buffer.remaining()) {
            needs = currentSize - buffer.remaining();
        }
        else {
            //reset back to size, field extractors will consume this
            buffer.position(buffer.position() - 4);
            this.accumulator = func.apply(accumulator, this);
            soFar += currentSize;
            ++callIndex;
            finishField();
        }
    }

    @Override
    public final void networkComplete(final ByteBuffer buffer) {
        this.buffer = buffer;
        needs = 0;
        
        while(needs == 0 && index < rowDescription.length()) {
            if(state == State.NOT_STARTED) {
                notStarted();
            }
            
            //if the field size cannot be read, return, there is nothing left to do
            if(state == State.INCOMPLETE) {
                incomplete();
            }

            if(state == State.FIXED) {
                fixed();
            }
            
            if(state == State.STREAMING) {
                stream();
            }
        }
    }

    private void finishField() {
        ++index;
        state = State.INCOMPLETE;
        currentSize = 0;
        currentSoFar = 0;
        fieldBuffer = null;
        decoder = null;
    }

    private void stream() {
        if(callIndex < index) {
            this.accumulator = func.apply(accumulator, this);
            ++callIndex;
        }
        
        //lock in limits if necessary;
        final int currentLimit = buffer.limit();
        if(getCurrentLeft() < buffer.remaining()) {
            buffer.limit(buffer.position() + getCurrentLeft());
        }

        final int inBuffer = buffer.remaining();
        currentSoFar += inBuffer;
        soFar += inBuffer;
        if(fieldBuffer instanceof ByteBuffer) {
            ((ByteBuffer) fieldBuffer).put(buffer);
            if(getCurrentLeft() == 0) {
                finishField();
            }
        }
        else {
            CharBuffer cbuf = (CharBuffer) fieldBuffer;
            decoder.decode(buffer, cbuf, false);
            if(getCurrentLeft() == 0) {
                decoder.decode(buffer, cbuf, true);
                decoder.flush(cbuf);
                finishField();
            }
        }

        needs = getCurrentLeft();
        buffer.limit(currentLimit);
    }

    public boolean isStreaming() {
        return state == State.STREAMING;
    }
    
    public FieldDescriptor getFieldDescriptor() {
        return rowDescription.field(index);
    }

    private void assignTarget(final Buffer target) {
        this.fieldBuffer = target;
        this.currentSize = buffer.getInt();
        soFar += 4;
        stream();
    }
    
    public void stream(final ByteBuffer target) {
        assignTarget(target);
    }

    public void stream(final CharBuffer target) {
        this.decoder = SerializationContext.stringOps().getEncoding().newDecoder();
        assignTarget(target);
    }
    
    public Object asObject() {
        final int beginAt = buffer.position();
        final Object ret = DataRow.extractByPgType(rowDescription.field(index), buffer);
        soFar += (buffer.position() - beginAt);
        return ret;
    }

    //TODO: Add guards for invalid read types. Do the same for Data Row implementation.
    public String asString() {
        String ret;
        final int beginAt = buffer.position();
        final int size = buffer.getInt();
        if(size == -1) {
            ret = null;
        }
        else {
            ret = SerializationContext.bufferToString(size, buffer);
        }

        soFar += (buffer.position() - beginAt);
        return ret;
    }
    
    public boolean asBoolean() {
        final int beginAt = buffer.position();
        buffer.getInt();
        final boolean ret = readBoolean(buffer);
        soFar += (buffer.position() - beginAt);
        return ret;
    }
    
    public double asDouble() {
        final int beginAt = buffer.position();
        buffer.getInt();
        final double ret = readDouble(buffer);
        soFar += (buffer.position() - beginAt);
        return ret;
    }
    
    public float asFloat() {
        final int beginAt = buffer.position();
        buffer.getInt();
        final float ret = readFloat(buffer);
        soFar += (buffer.position() - beginAt);
        return ret;
    }
    
    public int asInt() {
        final int beginAt = buffer.position();
        buffer.getInt();
        final int ret = readInt(buffer);
        soFar += (buffer.position() - beginAt);
        return ret;
    }
    
    public long asLong() {
        final int beginAt = buffer.position();
        buffer.getInt();
        final long ret = readLong(buffer);
        soFar += (buffer.position() - beginAt);
        return ret;
    }
    
    public short asShort() {
        final int beginAt = buffer.position();
        buffer.getInt();
        final short ret = readShort(buffer);
        soFar += (buffer.position() - beginAt);
        return ret;
    }
    
    public Object asArray(final Class elementType) {
        final FieldDescriptor fd = rowDescription.field(index);
        final PgType pgType = registry.pgType(fd.getTypeOid());
        final int beginAt = buffer.position();
        final Object ret = pgType.read(buffer, fd.getTypeOid(), elementType);
        soFar += (buffer.position() - beginAt);
        return ret;
    }
}
