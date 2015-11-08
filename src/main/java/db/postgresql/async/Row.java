package db.postgresql.async;

public interface Row {

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
        boolean nextBoolean();
        double nextDouble();
        float nextFloat();
        int nextInt();
        long nextLong();
        short nextShort();
    }

    Extractor extractor();
    Iterator iterator();
}
