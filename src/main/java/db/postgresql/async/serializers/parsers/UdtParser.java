package db.postgresql.async.serializers.parsers;

import db.postgresql.async.types.Udt;
import db.postgresql.async.types.UdtInput;
import db.postgresql.async.serializers.SerializationContext;
import db.postgresql.async.serializers.Serializer;
import db.postgresql.async.serializers.BooleanSerializer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.BiFunction;

public class UdtParser<T extends CompositeMeta> implements UdtInput {

    private final CompositeEngine<T> engine;
    
    private UdtParser(final CharSequence buffer, final CompositeFactory<T> factory) {
        this.engine = new CompositeEngine<>(buffer, factory);
    }

    public static UdtParser forUdt(final CharSequence buffer) {
        return new UdtParser<>(buffer, CompositeMeta.udt);
    }
 
    public static UdtParser forGeometry(final CharSequence buffer) {
        return new UdtParser<>(buffer, CompositeMeta.geometry);
    }
    
    private static final Class[] CONSTRUCTOR_ARGS = new Class[] { UdtInput.class };
    
    public <U> U read(Class<U> type) {
        if(Udt.class.isAssignableFrom(type)) {
            return readUdt(type);
        }
        else {
            final String val = engine.getField();
            final Serializer<U> serializer = SerializationContext.registry().serializer(type);
            if(val == null) {
                return null;
            }
            else {
                return serializer.fromString(val);
            }
        }
    }

    private static boolean isNull(final String val) {
        return val == null || val.length() == 0;
    }
    
    public boolean readBoolean() {
        final String val = engine.getField();
        if(isNull(val)) {
            return false;
        }
        else {
            return val.equals(BooleanSerializer.T) ? true : false;
        }
    }

    public short readShort() {
        final String val = engine.getField();
        return isNull(val) ? 0 : Short.parseShort(val);
    }

    public int readInt() {
        final String val = engine.getField();
        return isNull(val) ? 0 : Integer.parseInt(val);
    }

    public long readLong() {
        final String val = engine.getField();
        return isNull(val) ? 0L : Long.parseLong(val);
    }

    public float readFloat() {
        final String val = engine.getField();
        return isNull(val) ? 0f : Float.parseFloat(val);
    }

    public double readDouble() {
        final String val = engine.getField();
        return isNull(val) ? 0d : Double.parseDouble(val);
    }
    
    private <U> U readUdt(Class<U> type) {
        try {
            engine.beginUdt();
            Constructor<U> constructor = type.getConstructor(CONSTRUCTOR_ARGS);
            U udt = (U) constructor.newInstance(this);
            engine.endUdt();
            return udt;
        }
        catch(NoSuchMethodException | InstantiationException |
              IllegalAccessException | InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }

    public char getCurrentDelimiter() {
        return engine.getLevel().getBegin();
    }

    public boolean hasNext() {
        return engine.getCurrent() != engine.getLevel().getEnd();
    }
}
