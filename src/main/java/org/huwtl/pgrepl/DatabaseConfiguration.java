package org.huwtl.pgrepl;

import java.util.Properties;

import static java.lang.String.format;
import static org.postgresql.PGProperty.*;

public record DatabaseConfiguration(
        String port,
        String host,
        String database,
        String username,
        String password) {
    private static final String REPLICATION_TYPE_TO_SUPPORT_LOGICAL_DECODING = "database";
    private static final String QUERY_MODE = "simple";
    private static final String MINIMUM_SERVER_VERSION = "11.0";

    public static Builder builder() {
        return new Builder();
    }

    public String jdbcUrl() {
        return format("jdbc:postgresql://%s:%s/%s", host, port, database);
    }

    public Properties replicationProperties() {
        var properties = getQueryConnectionProperties();
        REPLICATION.set(properties, REPLICATION_TYPE_TO_SUPPORT_LOGICAL_DECODING);
        PREFER_QUERY_MODE.set(properties, QUERY_MODE);
        return properties;
    }

    public Properties getQueryConnectionProperties() {
        var properties = new Properties();
        USER.set(properties, username);
        PASSWORD.set(properties, password);
        ASSUME_MIN_SERVER_VERSION.set(properties, MINIMUM_SERVER_VERSION);
        return properties;
    }

    public static class Builder {
        private String port;
        private String host;
        private String database;
        private String username;
        private String password;

        Builder() {
        }

        public Builder port(String port) {
            this.port = port;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public DatabaseConfiguration build() {
            return new DatabaseConfiguration(port, host, database, username, password);
        }
    }
}
