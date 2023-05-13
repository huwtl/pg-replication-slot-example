package org.huwtl.pgrepl.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.Logger;
import org.huwtl.pgrepl.DatabaseConfiguration;
import org.huwtl.pgrepl.ObjectMapperFactory;
import org.huwtl.pgrepl.ReplicationConfiguration;
import org.huwtl.pgrepl.publisher.Data;
import org.huwtl.pgrepl.publisher.Publisher;
import org.postgresql.replication.LogSequenceNumber;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.logging.log4j.LogManager.getLogger;

public class PostgresDataChangeConsumer implements AutoCloseable {
    private static final Logger LOGGER = getLogger();
    private static final int SHUTDOWN_TIMEOUT_IN_SECONDS = 5;

    private final Publisher publisher;
    private final DatabaseConfiguration databaseConfiguration;
    private final ReplicationConfiguration replicationConfig;
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;

    public PostgresDataChangeConsumer(
            Publisher publisher,
            DatabaseConfiguration databaseConfiguration,
            ReplicationConfiguration replicationConfig) throws SQLException {
        this.publisher = publisher;
        this.databaseConfiguration = databaseConfiguration;
        this.replicationConfig = replicationConfig;
        this.objectMapper = ObjectMapperFactory.objectMapper();
        this.executorService = Executors.newSingleThreadExecutor();
    }

    public Future<Boolean> start() {
        addShutdownHook();

        return executorService.submit(() -> {
            LOGGER.info("Consuming from replication slot {}", replicationSlotName());
            while (!executorService.isShutdown()) {
                try (var postgresConnector = new PostgresConnector(databaseConfiguration, replicationConfig)) {
                    consumeAndPublishChanges(postgresConnector);
                } catch (Exception e) {
                    LOGGER.error("Unexpected error while consuming data changes", e);
                    waitAfterNoDataReceivedToGiveThreadSomePeaceAndTranquillity(replicationConfig);
                }
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
        LOGGER.info("Successfully shutdown consumer");
    }

    private void consumeAndPublishChanges(PostgresConnector postgresConnector) throws SQLException, IOException {
        while (!executorService.isShutdown()) {
            consumeAndPublishNextChange(postgresConnector);
        }
    }

    private void consumeAndPublishNextChange(PostgresConnector postgresConnector) throws SQLException, IOException {
        var buffer = postgresConnector.readPending();
        var lastReceivedLogSequenceNumber = postgresConnector.lastReceivedLogSequenceNumber();
        if (buffer != null) {
            var offset = buffer.arrayOffset();
            var bytes = buffer.array();
            var slotMessage = objectMapper.readValue(bytes, offset, bytes.length, SlotMessage.class);
            LOGGER.info("pending changes received: {} with lsn {}", slotMessage, lastReceivedLogSequenceNumber);
            slotMessage
                    // todo hlewis: we should ideally update the lsn for any slot message not matching this filter. The
                    //  only time we wouldn't want to update the lsn for a slot message is for failed publishes
                    .filterInsertsBySchemaAndTable(
                            replicationConfig.schemaNameToDetectChangesFrom(),
                            replicationConfig.tableNameToDetectChangesFrom()
                    )
                    .forEach(data -> publishDataChange(data, lastReceivedLogSequenceNumber, postgresConnector));
        } else {
            LOGGER.info("no changes received with lsn {}", lastReceivedLogSequenceNumber);
            postgresConnector.updateLogSequenceNumber(lastReceivedLogSequenceNumber);
            waitAfterNoDataReceivedToGiveThreadSomePeaceAndTranquillity(replicationConfig);
        }
    }

    private void publishDataChange(
            Data data,
            LogSequenceNumber lastReceivedLogSequenceNumber,
            PostgresConnector postgresConnector) {
        publisher.publish(data);
        postgresConnector.updateLogSequenceNumber(lastReceivedLogSequenceNumber);
    }

    private void waitAfterNoDataReceivedToGiveThreadSomePeaceAndTranquillity(
            ReplicationConfiguration replicationConfig) {
        try {
            Thread.sleep(replicationConfig.pollingIntervalInMillis());
        } catch (InterruptedException e) {
            LOGGER.error("interrupted while waiting during polling interval", e);
            throw new RuntimeException(e);
        }
    }

    private String replicationSlotName() {
        return replicationConfig.slotName();
    }

    private void addShutdownHook() {
        getRuntime().addShutdownHook(new Thread(this::close));
    }
}
