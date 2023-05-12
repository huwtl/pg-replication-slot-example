package org.huwtl.pgrepl;

import java.util.Properties;

public record ReplicationConfiguration(
        String slotName,
        String schemaNameToDetectChangesFrom,
        String tableNameToDetectChangesFrom) {
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
        private String slotName;
        private String schemaNameToDetectChangesFrom;
        private String tableNameToDetectChangesFrom;

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

        public ReplicationConfiguration build() {
            return new ReplicationConfiguration(
                    slotName,
                    schemaNameToDetectChangesFrom,
                    tableNameToDetectChangesFrom
            );
        }
    }
}
