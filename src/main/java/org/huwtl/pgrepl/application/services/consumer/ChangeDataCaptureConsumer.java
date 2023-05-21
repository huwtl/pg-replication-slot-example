package org.huwtl.pgrepl.application.services.consumer;

import org.apache.logging.log4j.Logger;
import org.huwtl.pgrepl.ReplicationConfiguration;
import org.huwtl.pgrepl.application.services.DelayService;
import org.huwtl.pgrepl.application.services.ThreadSleepingService;
import org.huwtl.pgrepl.application.services.publisher.Publisher;
import org.huwtl.pgrepl.application.services.replication.ReplicationStream;
import org.huwtl.pgrepl.application.services.replication.ReplicationStreamProvider;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.Runtime.getRuntime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.logging.log4j.LogManager.getLogger;

public class ChangeDataCaptureConsumer implements AutoCloseable {
    private static final Logger LOGGER = getLogger();
    private static final int SHUTDOWN_TIMEOUT_IN_SECONDS = 5;

    private final Publisher publisher;
    private final ReplicationConfiguration replicationConfig;
    private final ReplicationStreamProvider replicationStreamProvider;
    private final ExecutorService executorService;
    private final DelayService delayService;

    public ChangeDataCaptureConsumer(
            Publisher publisher,
            ReplicationConfiguration replicationConfig,
            ReplicationStreamProvider replicationStreamProvider) throws SQLException {
        this(
                publisher,
                replicationConfig,
                replicationStreamProvider,
                Executors.newSingleThreadExecutor(),
                new ThreadSleepingService()
        );
    }

    ChangeDataCaptureConsumer(
            Publisher publisher,
            ReplicationConfiguration replicationConfig,
            ReplicationStreamProvider replicationStreamProvider,
            ExecutorService executorService,
            DelayService delayService) {
        this.publisher = requireNonNull(publisher);
        this.replicationConfig = requireNonNull(replicationConfig);
        this.replicationStreamProvider = requireNonNull(replicationStreamProvider);
        this.executorService = requireNonNull(executorService);
        this.delayService = requireNonNull(delayService);
    }

    public Future<Boolean> start() {
        addShutdownHook();

        return executorService.submit(() -> {
            LOGGER.info("Consuming from replication slot {}", replicationSlotName());
            while (!executorService.isShutdown()) {
                openReplicationStreamAndConsumeChanges();
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

    private void openReplicationStreamAndConsumeChanges() {
        try (var replicationStream = replicationStreamProvider.openedReplicationStream()) {
            consumeAndPublishChanges(replicationStream);
        } catch (Exception e) {
            LOGGER.error("Unexpected error while consuming data changes", e);
            applyPollingDelay();
        }
    }

    private void consumeAndPublishChanges(ReplicationStream replicationStream) throws SQLException, IOException {
        while (!executorService.isShutdown()) {
            consumeAndPublishNextChange(replicationStream);
        }
    }

    private void consumeAndPublishNextChange(ReplicationStream replicationStream) throws SQLException, IOException {
        replicationStream.processNextChangeDataCaptureMessage(
                changeDataCaptureMessage -> changeDataCaptureMessage
                        .filterInsertsBySchemaAndTable(
                                replicationConfig.schemaNameToDetectChangesFrom(),
                                replicationConfig.tableNameToDetectChangesFrom()
                        )
                        .forEach(publisher::publish),
                noMessage -> applyPollingDelay()
        );
    }

    private void applyPollingDelay() {
        try {
            delayService.delayThreadForMillis(replicationConfig.pollingIntervalInMillis());
        } catch (InterruptedException e) {
            LOGGER.error("interrupted while delaying during polling interval", e);
        }
    }

    private String replicationSlotName() {
        return replicationConfig.slotName();
    }

    private void addShutdownHook() {
        getRuntime().addShutdownHook(new Thread(this::close));
    }
}
