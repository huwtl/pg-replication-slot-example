package org.huwtl.pgrepl.consumer;

import org.apache.logging.log4j.Logger;
import org.huwtl.pgrepl.DatabaseConfiguration;
import org.huwtl.pgrepl.ReplicationConfiguration;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.util.PSQLException;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.logging.log4j.LogManager.getLogger;

class PostgresConnector implements AutoCloseable {
    private static final Logger LOGGER = getLogger();
    private static final String ALREADY_EXISTS_SQL_STATE = "42710";
    private static final String CURRENTLY_RUNNING_PROCESS_ON_SLOT_SQL_STATE = "55006";

    private final Connection replicationConnection;
    private final PGReplicationStream replicationStream;

    PostgresConnector(DatabaseConfiguration postgresConfig, ReplicationConfiguration replicationConfig)
            throws SQLException {
        LOGGER.info("Connecting to {}", postgresConfig.jdbcUrl());
        replicationConnection = newConnection(postgresConfig.jdbcUrl(), postgresConfig.replicationProperties());
        LOGGER.info("Connected to postgres");
        var postgresReplicationApi = replicationConnection
                .unwrap(PGConnection.class)
                .getReplicationAPI();
        createReplicationSlot(replicationConfig, postgresReplicationApi);
        replicationStream = replicationStream(replicationConfig, postgresReplicationApi);
    }

    ByteBuffer read() throws SQLException {
        return replicationStream.read();
    }

    LogSequenceNumber lastReceivedLogSequenceNumber() {
        return replicationStream.getLastReceiveLSN();
    }

    void updateLogSequenceNumber(LogSequenceNumber logSequenceNumber) {
        replicationStream.setAppliedLSN(logSequenceNumber);
        replicationStream.setFlushedLSN(logSequenceNumber);
    }

    @Override
    public void close() {
        try {
            if (!replicationStream.isClosed()) {
                replicationStream.forceUpdateStatus();
                replicationStream.close();
            }
        } catch (SQLException e) {
            LOGGER.error("Unable to close replication stream", e);
        }
        try {
            if (!replicationConnection.isClosed()) {
                replicationConnection.close();
            }
        } catch (SQLException e) {
            LOGGER.error("Unable to close postgres streaming connection", e);
        }
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
                    .withSlotOptions(replicationConfig.slotOptions())
                    .start();
        } catch (PSQLException e) {
            if (e.getSQLState().equals(CURRENTLY_RUNNING_PROCESS_ON_SLOT_SQL_STATE)) {
                var slotName = replicationConfig.slotName();
                LOGGER.info("Replication slot {} currently has another process consuming from it", slotName);
            }
            throw e;
        }
    }

    private Connection newConnection(String url, Properties properties) throws SQLException {
        return DriverManager.getConnection(url, properties);
    }
}
