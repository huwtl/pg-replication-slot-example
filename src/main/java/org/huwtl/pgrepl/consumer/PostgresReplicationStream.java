package org.huwtl.pgrepl.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.Logger;
import org.huwtl.pgrepl.DatabaseConfiguration;
import org.huwtl.pgrepl.ObjectMapperFactory;
import org.huwtl.pgrepl.ReplicationConfiguration;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.util.PSQLException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.logging.log4j.LogManager.getLogger;
import static org.huwtl.pgrepl.consumer.ReplicationStreamMessage.ChangeDataCaptureMessage;
import static org.huwtl.pgrepl.consumer.ReplicationStreamMessage.NoMessage;

public class PostgresReplicationStream implements ReplicationStream {
    private static final Logger LOGGER = getLogger();
    private static final String ALREADY_EXISTS_SQL_STATE = "42710";
    private static final String CURRENTLY_RUNNING_PROCESS_ON_SLOT_SQL_STATE = "55006";

    private final Connection replicationConnection;
    private final PGReplicationStream replicationStream;
    private final ObjectMapper objectMapper;

    public PostgresReplicationStream(DatabaseConfiguration postgresConfig, ReplicationConfiguration replicationConfig)
            throws SQLException {
        LOGGER.info("Connecting to {}", postgresConfig.jdbcUrl());
        replicationConnection = newConnection(postgresConfig.jdbcUrl(), postgresConfig.replicationProperties());
        LOGGER.info("Connected to postgres");
        var postgresReplicationApi = replicationConnection
                .unwrap(PGConnection.class)
                .getReplicationAPI();
        createReplicationSlot(replicationConfig, postgresReplicationApi);
        replicationStream = replicationStream(replicationConfig, postgresReplicationApi);
        objectMapper = ObjectMapperFactory.objectMapper();
    }

    @Override
    public void processNextChangeDataCaptureMessage(
            Consumer<ChangeDataCaptureMessage> onChangeDataCaptureMessage,
            Consumer<NoMessage> onNoMessage) throws SQLException, IOException {
        var oldLsn = lastReceivedLogSequenceNumber();
        var buffer = replicationStream.readPending();
        var newLsn = lastReceivedLogSequenceNumber();
        if (buffer != null) {
            var offset = buffer.arrayOffset();
            var bytes = buffer.array();
            var slotMessage = objectMapper.readValue(bytes, offset, bytes.length, SlotMessage.class);
            LOGGER.info("pending changes received {} with lsn {}", slotMessage, newLsn);
            onChangeDataCaptureMessage.accept(slotMessage);
            updateLogSequenceNumber(newLsn);
        } else if (!newLsn.equals(oldLsn)) {
            LOGGER.info("keepalive message received with lsn {}", newLsn);
            updateLogSequenceNumber(newLsn);
        } else {
            onNoMessage.accept(new NoMessage());
        }
    }

    @Override
    public void close() {
        try {
            if (replicationStream != null && !replicationStream.isClosed()) {
                replicationStream.forceUpdateStatus();
                replicationStream.close();
                LOGGER.info("Replication stream closed");
            }
        } catch (SQLException e) {
            LOGGER.error("Unable to close replication stream", e);
        }
        try {
            if (replicationConnection != null && !replicationConnection.isClosed()) {
                replicationConnection.close();
                LOGGER.info("Replication connection closed");
            }
        } catch (SQLException e) {
            LOGGER.error("Unable to close postgres streaming connection", e);
        }
    }

    private LogSequenceNumber lastReceivedLogSequenceNumber() {
        return replicationStream.getLastReceiveLSN();
    }

    private void updateLogSequenceNumber(LogSequenceNumber logSequenceNumber) {
        replicationStream.setAppliedLSN(logSequenceNumber);
        replicationStream.setFlushedLSN(logSequenceNumber);
    }

    private void createReplicationSlot(
            ReplicationConfiguration replicationConfig,
            PGReplicationConnection postgresReplicationApi) throws SQLException {
        var replicationSlotName = replicationConfig.slotName();
        try {
            LOGGER.info("Attempting to create replication slot {}", replicationSlotName);
            postgresReplicationApi.createReplicationSlot()
                    .logical()
                    .withOutputPlugin(replicationConfig.outputPlugin())
                    .withSlotName(replicationSlotName)
                    .make();
            LOGGER.info("Created replication slot {}", replicationSlotName);
        } catch (SQLException e) {
            if (e.getSQLState().equals(ALREADY_EXISTS_SQL_STATE)) {
                LOGGER.info("Slot {} already exists", replicationSlotName);
            } else {
                throw (e);
            }
        }
    }

    private PGReplicationStream replicationStream(
            ReplicationConfiguration replicationConfig,
            PGReplicationConnection replicationApi) throws SQLException {
        try {
            return replicationApi
                    .replicationStream()
                    .logical()
                    .withSlotName(replicationConfig.slotName())
                    .withStatusInterval(replicationConfig.statusIntervalInMillis(), MILLISECONDS)
                    .withSlotOptions(replicationConfig.slotOptions())
                    .start();
        } catch (PSQLException e) {
            if (e.getSQLState().equals(CURRENTLY_RUNNING_PROCESS_ON_SLOT_SQL_STATE)) {
                var slotName = replicationConfig.slotName();
                LOGGER.info("Replication slot {} currently has another process consuming from it", slotName);
                close();
            }
            throw e;
        }
    }

    private Connection newConnection(String url, Properties properties) throws SQLException {
        return DriverManager.getConnection(url, properties);
    }
}
