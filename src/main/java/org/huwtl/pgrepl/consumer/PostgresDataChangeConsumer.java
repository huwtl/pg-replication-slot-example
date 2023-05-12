package org.huwtl.pgrepl.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.Logger;
import org.huwtl.pgrepl.DatabaseConfiguration;
import org.huwtl.pgrepl.ObjectMapperFactory;
import org.huwtl.pgrepl.ReplicationConfiguration;
import org.huwtl.pgrepl.publisher.Data;
import org.huwtl.pgrepl.publisher.Publisher;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.logging.log4j.LogManager.getLogger;

public class PostgresDataChangeConsumer implements AutoCloseable {
    private static final Logger LOGGER = getLogger();
    private static final int SHUTDOWN_TIMEOUT_IN_SECONDS = 5;

    private final Publisher publisher;
    private final ReplicationConfiguration replicationConfig;
    private final PostgresConnector postgresConnector;
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;

    public PostgresDataChangeConsumer(
            Publisher publisher,
            DatabaseConfiguration postgresConfig,
            ReplicationConfiguration replicationConfig) throws SQLException {
        this.publisher = publisher;
        this.replicationConfig = replicationConfig;
        this.postgresConnector = new PostgresConnector(postgresConfig, replicationConfig);
        this.objectMapper = ObjectMapperFactory.objectMapper();
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public Future<Boolean> start() {
        return executorService.submit(() -> {
            LOGGER.info("Consuming from replication slot {}", replicationSlotName());
            while (!executorService.isShutdown()) {
                consumeAndPublishChanges();
            }
            return true;
        });
    }

    @Override
    public void close() {
        LOGGER.info("Attempting to shutdown consumer");
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(SHUTDOWN_TIMEOUT_IN_SECONDS, SECONDS)) {
                LOGGER.error("Forcing consumer to shutdown since taking too long");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOGGER.error("Unexpected error during consumer shutdown", e);
            executorService.shutdownNow();
        }
        postgresConnector.close();
        LOGGER.info("Successfully shutdown consumer");
    }

    private void consumeAndPublishChanges() {
        try {
            consumeAndPublishChanges(postgresConnector);
        } catch (Exception e) {
            LOGGER.error("unexpected exception when consuming from replication slot {}", replicationSlotName(), e);
        }
    }

    private void consumeAndPublishChanges(PostgresConnector postgresConnector) throws SQLException, IOException {
        var buffer = postgresConnector.read();
        if (buffer != null) {
            var offset = buffer.arrayOffset();
            var bytes = buffer.array();
            var slotMessage = objectMapper.readValue(bytes, offset, bytes.length, SlotMessage.class);
            LOGGER.info("pending changes: {}", slotMessage);
            slotMessage
                    .filterInsertsBySchemaAndTable(
                            replicationConfig.schemaNameToDetectChangesFrom(),
                            replicationConfig.tableNameToDetectChangesFrom()
                    )
                    .forEach(data -> publishDataChange(data, postgresConnector));
        }
    }

    private void publishDataChange(Data data, PostgresConnector postgresConnector) {
        var lastReceivedLogSequenceNumber = postgresConnector.lastReceivedLogSequenceNumber();
        try {
            publisher.publish(data);
            postgresConnector.updateLogSequenceNumber(lastReceivedLogSequenceNumber);
        } catch (Exception e) {
            LOGGER.error("unexpected exception when publishing data change for log sequence number {}",
                    lastReceivedLogSequenceNumber, e);
        }
    }

    private String replicationSlotName() {
        return replicationConfig.slotName();
    }
}
