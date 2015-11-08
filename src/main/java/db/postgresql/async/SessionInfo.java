package db.postgresql.async;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import db.postgresql.async.pginfo.Registry;
import db.postgresql.async.pginfo.BootstrapRegistry;
import db.postgresql.async.pginfo.PgTypeRegistry;

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

    public Map<String,String> getInitKeysValues() {
        Map<String,String> ret = new LinkedHashMap<>();
        ret.put("user", user);
        ret.put("database", getDatabase());
        ret.put("application_name", getApplication());
        ret.put("client_encoding", getPostgresEncoding());
        return Collections.unmodifiableMap(ret);
    }

    private final Registry bootstrapRegistry;
    public Registry getBootstrapRegistry() { return bootstrapRegistry; }

    private final Registry mainRegistry;
    public Registry getMainRegistry() { return mainRegistry; }

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
        this.bootstrapRegistry = builder.bootstrapRegistry;
        this.mainRegistry = builder.mainRegistry;
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
        private Registry bootstrapRegistry = new BootstrapRegistry();
        private Registry mainRegistry = new PgTypeRegistry();

        public Builder() { }

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

        public Builder bootstrapRegistry(final Registry val) {
            this.bootstrapRegistry = val;
            return this;
        }

        public Builder mainRegistry(final Registry val) {
            this.mainRegistry = val;
            return this;
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
