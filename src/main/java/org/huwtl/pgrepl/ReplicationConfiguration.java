package org.huwtl.pgrepl;

import java.util.Properties;

public record ReplicationConfiguration(
        String slotName,
        String schemaNameToDetectChangesFrom,
        String tableNameToDetectChangesFrom,
        int statusIntervalInMillis,
        long pollingIntervalInMillis) {
    private static final String OUTPUT_PLUGIN = "wal2json";
    private static final String INCLUDE_XIDS = "true";

    public static Builder builder() {
        return new Builder();
    }

    public String outputPlugin() {
        return OUTPUT_PLUGIN;
    }

    public Properties slotOptions() {
        var properties = new Properties();
        properties.setProperty("include-xids", INCLUDE_XIDS);
        return properties;
    }

    public static class Builder {
        private static final int DEFAULT_STATUS_INTERVAL_IN_MILLIS = 5000;
        private static final long DEFAULT_POLLING_INTERVAL_IN_MILLIS = 1000;

        private String slotName;
        private String schemaNameToDetectChangesFrom;
        private String tableNameToDetectChangesFrom;
        private int statusIntervalInMillis = DEFAULT_STATUS_INTERVAL_IN_MILLIS;
        private long pollingIntervalInMillis = DEFAULT_POLLING_INTERVAL_IN_MILLIS;

        public Builder slotName(String slotName) {
            this.slotName = slotName;
            return this;
        }

        public Builder schemaNameToDetectChangesFrom(String schemaNameToDetectChangesFrom) {
            this.schemaNameToDetectChangesFrom = schemaNameToDetectChangesFrom;
            return this;
        }

        public Builder tableNameToDetectChangesFrom(String tableNameToDetectChangesFrom) {
            this.tableNameToDetectChangesFrom = tableNameToDetectChangesFrom;
            return this;
        }

        public Builder statusIntervalInMillis(int statusIntervalInMillis) {
            this.statusIntervalInMillis = statusIntervalInMillis;
            return this;
        }

        public Builder pollingIntervalInMillis(long pollingIntervalInMillis) {
            this.pollingIntervalInMillis = pollingIntervalInMillis;
            return this;
        }

        public ReplicationConfiguration build() {
            return new ReplicationConfiguration(
                    slotName,
                    schemaNameToDetectChangesFrom,
                    tableNameToDetectChangesFrom,
                    statusIntervalInMillis,
                    pollingIntervalInMillis
            );
        }
    }
}
