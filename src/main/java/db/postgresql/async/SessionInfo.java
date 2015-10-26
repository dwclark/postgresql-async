package db.postgresql.async;

import java.nio.charset.Charset;
import java.util.Locale;

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

    private final int channels;
    public int getChannels() { return channels; }

    private SessionInfo(final String user,
                        final String password,
                        final String database,
                        final String host,
                        final int port,
                        final String application,
                        final Charset encoding,
                        final String postgresEncoding,
                        final boolean ssl,
                        final Locale numeric,
                        final Locale money,
                        final int channels) {
                        
        this.user = user;
        this.password = password;
        this.database = database;
        this.host = host;
        this.port = port;
        this.application = application;
        this.encoding = encoding;
        this.postgresEncoding = postgresEncoding;
        this.ssl = ssl;
        this.numeric = numeric;
        this.money = money;
        this.channels = channels;
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
        private int channels = 1;

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

        public Builder channels(final int val) { channels = val; return this; }

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

            return new SessionInfo(user, password,
                                   database, host,
                                   port, application,
                                   encoding, postgresEncoding,
                                   ssl, numeric,
                                   money, channels);
        }
    }
}
