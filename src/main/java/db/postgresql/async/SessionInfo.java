package db.postgresql.async;

import db.postgresql.async.pginfo.PgTypeRegistry;
import db.postgresql.async.pginfo.Registry;
import db.postgresql.async.serializers.*;
import db.postgresql.async.types.*;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import db.postgresql.async.serializers.PostgresNumeric;
import static db.postgresql.async.serializers.SerializationContext.*;

public class SessionInfo {

    private final String user;
    public String getUser() { return user; }
    
    private final String password;
    public String getPassword() { return password; }
    
    private final String database;
    public String getDatabase() { return database; }
    
    private final String host;
    public String getHost() { return host; }
    
    private final int port;
    public int getPort() { return port; }
    
    private final String application;
    public String getApplication() { return application; }
    
    private final Charset encoding;
    public Charset getEncoding() { return encoding; }
    
    private final String postgresEncoding;
    public String getPostgresEncoding() { return postgresEncoding; }
        
    private final boolean ssl;
    public boolean getSsl() { return ssl; }
    
    private final Locale numeric;
    public Locale getNumeric() { return numeric; }
    
    private final Locale money;
    public Locale getMoney() { return money; }

    private final int minChannels;
    public int getMinChannels() { return minChannels; }

    private final int maxChannels;
    public int getMaxChannels() { return maxChannels; }

    private final long backOff;
    public long getBackOff() { return backOff; }

    private final TimeUnit backOffUnits;
    public TimeUnit getBackOffUnits() { return backOffUnits; }

    public SocketAddress getSocketAddress() {
        return new InetSocketAddress(host, port);
    }

    private final List<Mapping> mappings;
    
    public final List<Mapping> getMappings() {
        return mappings;
    }

    public Map<String,String> getInitKeysValues() {
        Map<String,String> ret = new LinkedHashMap<>();
        ret.put("user", user);
        ret.put("database", getDatabase());
        ret.put("application_name", getApplication());
        ret.put("client_encoding", getPostgresEncoding());
        return Collections.unmodifiableMap(ret);
    }

    private final PgTypeRegistry registry;
    public PgTypeRegistry getRegistry() { return registry; }

    private SessionInfo(final Builder builder) {
        this.user = builder.user;
        this.password = builder.password;
        this.database = builder.database;
        this.host = builder.host;
        this.port = builder.port;
        this.application = builder.application;
        this.encoding = builder.encoding;
        this.postgresEncoding = builder.postgresEncoding;
        this.ssl = builder.ssl;
        this.numeric = builder.numeric;
        this.money = builder.money;
        this.minChannels = builder.minChannels;
        this.maxChannels = builder.maxChannels;
        this.backOff = builder.backOff;
        this.backOffUnits = builder.backOffUnits;
        this.registry = builder.registry;
        this.mappings = Collections.unmodifiableList(builder.mappings);
    }

    public static class Builder {
        private String user;
        private String password;
        private String database;
        private String host = "localhost";
        private int port = 5432;
        private String application = "db.postgresql.asyc";
        private Charset encoding = Charset.forName("UTF-8");
        private String postgresEncoding = "UTF8";
        private boolean ssl = false;
        private Locale numeric = Locale.getDefault();
        private Locale money = Locale.getDefault();
        private int minChannels = 1;
        private int maxChannels = 1;
        private long backOff = 60L;
        private TimeUnit backOffUnits = TimeUnit.SECONDS;
        private PgTypeRegistry registry = new PgTypeRegistry();

        public void addDefaultMappings() {
            mapping(BigDecimal.class, "pg_catalog.numeric",
                    (buffer,obj) -> new PostgresNumeric((BigDecimal) obj).toBuffer(buffer),
                    (size,buffer,oid) -> new PostgresNumeric(buffer).toBigDecimal());
            
            mapping(BitSet.class, "pg_catalog.bit", BitSetSerializer::write, BitSetSerializer::read);
            
            mapping(BitSet.class, "pg_catalog.varbit", BitSetSerializer::write, BitSetSerializer::read);
            
            mapping(Boolean.class, "pg_catalog.bool",
                    (buffer,obj) -> buffer.put((byte) (((Boolean) obj) ? 1 : 0)),
                    (size,buffer,oid) -> buffer.get() == 1 ? true : false);
            
            mapping(Box.class, "pg_catalog.box", Box::write, Box::read);
            
            mapping(byte[].class, "pg_catalog.bytea",
                    (buffer,obj) -> buffer.put((byte[]) obj),
                    (size,buffer,oid) -> {
                        byte[] ret = new byte[size];
                        buffer.get(ret);
                        return ret;
                    });
            
            mapping(Circle.class, "pg_catalog.circle", Circle::write, Circle::read);
            
            //mapping(Cursor.class, "pg_catalog.refcursor", Cursor::write, Cursor::read);
            
            mapping(Double.class, "pg_catalog.float8",
                    (buffer,obj) -> buffer.putDouble((Double) obj),
                    (size,buffer,oid) -> buffer.getFloat());
            
            mapping(Float.class, "pg_catalog.float4",
                    (buffer,obj) -> buffer.putFloat((Float) obj),
                    (size,buffer,oid) -> buffer.getFloat());
            
            mapping(Inet.class, "pg_catalog.inet", Inet::write, Inet::read);
            
            mapping(Inet.class, "pg_catalog.cidr", Inet::write, Inet::read);
            
            mapping(Integer.class, "pg_catalog.int4",
                    (buffer,obj) -> buffer.putInt((Integer) obj),
                    (size,buffer,oid) -> buffer.getInt());
            
            mapping(Interval.class, "pg_catalog.interval", Interval::write, Interval::read);
            
            mapping(Jsonb.class, "pg_catalog.jsonb", Jsonb::write, Jsonb::read);
            
            mapping(Line.class, "pg_catalog.line", Line::write, Line::read);
            
            mapping(LineSegment.class, "pg_catalog.lseg", LineSegment::write, LineSegment::read);

            mapping(LocalDate.class, "pg_catalog.date",
                    (buffer,obj) -> buffer.putInt((int) PostgresDateTime.toDay((LocalDate) obj)),
                    (size,buffer,oid) -> PostgresDateTime.toLocalDate((long) buffer.getInt()));

            mapping(LocalDateTime.class, "pg_catalog.timestamp",
                    (buffer,obj) -> buffer.putLong(PostgresDateTime.toTimestamp((LocalDateTime) obj)),
                    (size,buffer,oid) -> PostgresDateTime.toLocalDateTime(buffer.getLong()));

            mapping(LocalTime.class, "pg_catalog.time",
                    (buffer,obj) -> buffer.putLong(PostgresDateTime.toTime((LocalTime) obj)),
                    (size,buffer,obj) -> PostgresDateTime.toLocalTime(buffer.getLong()));
            
            mapping(Long.class, "pg_catalog.int8",
                    (buffer,obj) -> buffer.putLong((Long) obj),
                    (size,buffer,oid) -> buffer.getLong());
            
            mapping(MacAddr.class, "pg_catalog.macaddr", MacAddr::write, MacAddr::read);
            
            mapping(Money.class, "pg_catalog.money", Money::write, Money::read);
            
            mapping(OffsetDateTime.class, "pg_catalog.timestamptz",
                    (buffer,obj) -> buffer.putLong(PostgresDateTime.toTimestamp((OffsetDateTime) obj)),
                    (size,buffer,oid) -> OffsetDateTime.of(PostgresDateTime.toLocalDateTime(buffer.getLong()), ZoneOffset.UTC));

            mapping(OffsetTime.class, "pg_catalog.timetz",
                    (buffer,obj) -> {
                        final OffsetTime ot = (OffsetTime) obj;
                        buffer.putLong(PostgresDateTime.toTime(ot.toLocalTime()));
                        buffer.putInt(PostgresDateTime.toPostgresOffset(ot.getOffset()));
                    },
                    (size,buffer,oid) -> PostgresDateTime.toOffsetTime(buffer.getLong(), buffer.getInt()));
            
            mapping(Path.class, "pg_catalog.path", Path::write, Path::read);
            
            mapping(Point.class, "pg_catalog.point", Point::write, Point::read);
            
            mapping(Polygon.class, "pg_catalog.polygon", Polygon::write, Polygon::read);
            
            mapping(Range.Int4.class, "pg_catalog.int4range", Range::write, Range.Int4::read);
            
            mapping(Short.class, "pg_catalog.int2", (b,o) -> b.putShort((Short) o), (s,b,o) -> b.getShort());

            final Mapping.Writer swriter = (buffer,obj) -> stringToBuffer(buffer, (String) obj);
            final Mapping.Reader sreader = (size,buffer,obj) -> bufferToString(size, buffer);
            for(String s : Arrays.asList("pg_catalog.text", "pg_catalog.varchar", "pg_catalog.xml",
                                         "pg_catalog.json", "pg_catalog.char", "pg_catalog.bpchar")) {
                mapping(String.class, s, swriter, sreader);
            }
            
            mapping(UUID.class, "pg_catalog.uuid",
                    (buffer,obj) -> {
                        UUID u = (UUID) obj;
                        buffer.putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits());
                    },
                    (size,buffer,oid) -> new UUID(buffer.getLong(), buffer.getLong()));
                            
            
            //record mappings will automatically get added for all complex types with
            //no associated mappings
        }
        
        private List<Mapping> mappings = new ArrayList<>();
        
        public Builder() {
            addDefaultMappings();
        }

        public Builder user(final String val) { user = val; return this; }

        public Builder password(final String val) { password = val; return this; }

        public Builder database(final String val) { database = val; return this; }

        public Builder host(final String val) { host = val; return this; }

        public Builder port(final int val) { port = val; return this; }

        public Builder application(final String val) { application = val; return this; }

        public Builder encoding(final Charset javaVal, final String postgresVal) {
            encoding = javaVal;
            postgresEncoding = postgresVal;
            return this;
        }

        public Builder ssl(final boolean val) { ssl = val; return this; }

        public Builder numeric(final Locale val) { numeric = val; return this; }

        public Builder money(final Locale val) { money = val; return this; }

        public Builder channels(final int min, final int max) {
            if(min <= 0) {
                throw new IllegalArgumentException("min must be greater than 0");
            }

            if(max <= 0) {
                throw new IllegalArgumentException("max must be greater than 0");
            }

            if(min >= max) {
                throw new IllegalArgumentException("min must be <= max");
            }
            
            minChannels = min;
            maxChannels = max;
            return this;
        }

        public Builder backOff(final long backOff, final TimeUnit units) {
            this.backOff = backOff;
            this.backOffUnits = units;
            return this;
        }

        public Builder registry(final PgTypeRegistry val) {
            this.registry = val;
            return this;
        }

        public Builder mapping(final Class type, final String name, final Mapping.Writer writer, final Mapping.Reader reader) {
            this.mappings.add(new Mapping(type, name, writer, reader));
            return this;
        }

        @SuppressWarnings("unchecked")
        public <E extends Enum<E>> void enumMapping(final Class<E> enumType, final String pgName) {
            final Mapping.Writer writer = (buffer, o) -> stringToBuffer(buffer, ((E) o).name());
            final Mapping.Reader reader = (size, buffer, oid) -> Enum.valueOf(enumType, bufferToString(size, buffer));
            mapping(enumType, pgName, writer, reader);
        }

        public SessionInfo build() {
            if(user == null) {
                throw new IllegalStateException("You must specify a user");
            }

            if(database == null) {
                throw new IllegalStateException("You must specify a database");
            }

            if(host == null) {
                throw new IllegalStateException("You must specify a host");
            }

            return new SessionInfo(this);
        }
    }
}
