package org.huwtl.pgrepl;

import org.apache.logging.log4j.core.tools.picocli.CommandLine;
import org.huwtl.pgrepl.application.services.consumer.ChangeDataCaptureConsumer;
import org.huwtl.pgrepl.application.services.publisher.CountingPublisher;
import org.huwtl.pgrepl.infrastructure.postgres.PostgresReplicationStream;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import static org.apache.logging.log4j.core.tools.picocli.CommandLine.Command;
import static org.apache.logging.log4j.core.tools.picocli.CommandLine.Option;

@SuppressWarnings("unused")
@Command(name = "Postgres replication test application")
public class Application implements Runnable {
    @Option(names = "--port", required = true, description = "database port")
    private String databasePort;
    @Option(names = "--host", required = true, description = "database host")
    private String databaseHost;
    @Option(names = "--database", required = true, description = "database name")
    private String databaseName;
    @Option(names = "--user", required = true, description = "database user")
    private String databaseUser;
    @Option(names = "--password", required = true, description = "database password")
    private String databasePassword;
    @Option(names = "--slot", required = true, description = "replication slot name")
    private String replicationSlotName;
    @Option(names = "--schema", required = true, description = "database schema to detect changes from")
    private String databaseSchemaNameToDetectChangesFrom;
    @Option(names = "--table", required = true, description = "database table to detect changes from")
    private String databaseTableNameToDetectChangesFrom;

    public static void main(String[] args) {
        CommandLine.run(new Application(), System.out, args);
    }

    @Override
    public void run() {
        var databaseConfig = DatabaseConfiguration.builder()
                .port(databasePort)
                .host(databaseHost)
                .database(databaseName)
                .username(databaseUser)
                .password(databasePassword)
                .build();
        ReplicationConfiguration replicationConfig = ReplicationConfiguration.builder()
                .slotName(replicationSlotName)
                .schemaNameToDetectChangesFrom(databaseSchemaNameToDetectChangesFrom)
                .tableNameToDetectChangesFrom(databaseTableNameToDetectChangesFrom)
                .build();
        try (var changeDataCaptureConsumer = new ChangeDataCaptureConsumer(
                new CountingPublisher(),
                replicationConfig,
                () -> new PostgresReplicationStream(databaseConfig, replicationConfig)
        )) {
            changeDataCaptureConsumer.start().get();
        } catch (SQLException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
