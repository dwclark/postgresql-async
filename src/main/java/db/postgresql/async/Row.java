package db.postgresql.async;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface Row {

    int length();
    String name(int index);
    String[] getNames();
    void finish();

    public interface Extractor {
        Object getAt(String field);
        Object getAt(int index);
        
        <T> T objectAt(Class<T> type, String field);
        <T> T objectAt(Class<T> type, int index);
        
        boolean booleanAt(String field);
        boolean booleanAt(int field);
        
        double doubleAt(String field);
        double doubleAt(int field);
        
        float floatAt(String field);
        float floatAt(int field);
        
        int intAt(String field);
        int intAt(int field);
        
        long longAt(String field);
        long longAt(int field);
        
        short shortAt(String field);
        short shortAt(int field);
    }

    public interface Iterator extends java.util.Iterator<Object> {
        <T> T next(Class<T> type);
        String nextString();
        boolean nextBoolean();
        double nextDouble();
        float nextFloat();
        int nextInt();
        long nextLong();
        short nextShort();
    }

    Extractor extractor();
    Iterator iterator();

    static Integer nullExecute(final Integer count, final Row row) {
        return count;
    }

    default void with(final Runnable runner) {
        try {
            runner.run();
        }
        finally {
            finish();
        }
    }

    default <T> T single(Class<T> type) {
        return iterator().next(type);
    }

    default Object single() {
        return iterator().next();
    }
    
    default Object[] toArray() {
        final Object[] ret = new Object[length()];
        final Iterator iter = iterator();
        int index = 0;
        while(iter.hasNext()) {
            ret[index++] = iter.next();
        }
        
        return ret;
    }
    
    default List<Object> toList() {
        return Arrays.asList(toArray());
    }

    default Map<String,Object> toMap() {
        final Map<String,Object> ret = new LinkedHashMap<>(length());
        final Iterator valueIterator = iterator();
        int index = 0;
        while(valueIterator.hasNext()) {
            ret.put(name(index++), valueIterator.next());
        }

        return ret;
    }

    default <T> T toObject(final Class<T> type) {
        return toObject(type, Row::nullTranslator);
    }

    static String nullTranslator(final String fieldName) {
        return fieldName;
    }
    
    default <T> T toObject(final Class<T> type, final Function<String,String> translator) {
        try {
            final T ret = type.newInstance();
            final Extractor e = extractor();
            for(int i = 0; i < length(); ++i) {
                final String fieldName = translator.apply(name(i));
                final PropertyDescriptor pd = new PropertyDescriptor(fieldName, type);
                pd.getWriteMethod().invoke(ret, e.getAt(i));
            }
            
            return ret;
        }
        catch(InstantiationException | IllegalAccessException |
              IntrospectionException | InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }

    static Object[] nullTranslator(final String[] names, final Object[] values) {
        return values;
    }

    default <T> T toImmutableObject(final Class<T> type) {
        return toImmutableObject(type, Row::nullTranslator);
    }

    default <T> T toImmutableObject(final Class<T> type, final BiFunction<String[],Object[],Object[]> translator) {
        try {
            final Object[] args = translator.apply(getNames(), toArray());
            final Class[] types = Arrays.stream(args).map((obj) -> obj.getClass()).toArray(Class[]::new);
            final Constructor<T> constructor = type.getDeclaredConstructor(types);
            return constructor.newInstance(args);
        }
        catch(NoSuchMethodException | InstantiationException |
              IllegalAccessException | InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }
}
