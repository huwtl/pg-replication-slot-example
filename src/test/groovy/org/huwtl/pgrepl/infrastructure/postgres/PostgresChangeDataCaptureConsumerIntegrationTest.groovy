package org.huwtl.pgrepl.infrastructure.postgres

import groovy.sql.Sql
import org.huwtl.pgrepl.DatabaseConfiguration
import org.huwtl.pgrepl.ReplicationConfiguration
import org.huwtl.pgrepl.application.services.consumer.ChangeDataCaptureConsumer
import org.huwtl.pgrepl.application.services.publisher.Data
import org.huwtl.pgrepl.application.services.publisher.ExceptionThrowingPublisher
import org.huwtl.pgrepl.application.services.publisher.InMemoryPublishedDataStore
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
import spock.util.concurrent.PollingConditions

class PostgresChangeDataCaptureConsumerIntegrationTest extends Specification {
    private static final String SCHEMA_NAME = "replication_test"
    private static final String TABLE_NAME = "events"
    private static final String REPLICATION_SLOT_NAME = "replication_test"
    private static final String CREATE_SCHEMA_SQL = "CREATE SCHEMA ${SCHEMA_NAME}"
    private static final String DROP_SCHEMA_SQL = "DROP SCHEMA ${SCHEMA_NAME} CASCADE"
    private static final String DROP_REPLICATION_SLOT = "SELECT pg_drop_replication_slot(?)"
    private static final String CREATE_TABLE_SQL = "CREATE TABLE ${TABLE_NAME}(id INT PRIMARY KEY, data TEXT NOT NULL)"
    private static final String INSERT_SQL = "INSERT INTO ${TABLE_NAME}(id, data) VALUES(?, ?)"
    private static final String UPDATE_DATA_BY_ID_SQL = "UPDATE ${TABLE_NAME} SET data = ? WHERE id = ?"
    private static final String DELETE_BY_ID_SQL = "DELETE FROM ${TABLE_NAME} WHERE id = ?"
    private static final String DELETE_ALL_SQL = "DELETE FROM ${TABLE_NAME}"
    private static final String OTHER_DATABASE_NAME = "replication_test_db_other"
    private static final String OTHER_SCHEMA_NAME = "replication_test_other"
    private static final String CREATE_OTHER_DATABASE_SQL = "CREATE DATABASE ${OTHER_DATABASE_NAME}"
    private static final String CREATE_OTHER_SCHEMA_SQL = "CREATE SCHEMA ${OTHER_SCHEMA_NAME}"
    private static final String OTHER_TABLE_NAME = "events_other"
    private static final String CREATE_OTHER_TABLE_SQL = "CREATE TABLE ${OTHER_TABLE_NAME}(id INT PRIMARY KEY, data TEXT NOT NULL)"
    private static final String OTHER_INSERT_SQL = "INSERT INTO ${OTHER_TABLE_NAME}(id, data) VALUES(?, ?)"
    private static final int ASYNC_ASSERTION_TIMEOUT_IN_SECS = 5

    @Shared
    @AutoCleanup
    private Sql sql
    @Shared
    @AutoCleanup
    private Sql sqlForOtherDatabase
    @Shared
    @AutoCleanup
    private EmbeddedPostgresContainer database
    @Shared
    private InMemoryPublishedDataStore inMemoryPublisher
    @Shared
    private ExceptionThrowingPublisher exceptionThrowingPublisher
    @Shared
    private DatabaseConfiguration databaseConfig
    @Shared
    private ReplicationConfiguration replicationConfig

    @AutoCleanup
    private ChangeDataCaptureConsumer consumer

    def setupSpec() {
        database = new EmbeddedPostgresContainer()
        inMemoryPublisher = new InMemoryPublishedDataStore()
        exceptionThrowingPublisher = ExceptionThrowingPublisher.willNotThrowException(inMemoryPublisher)
        database.sqlForDefaultDatabaseAndSchema().tap {
            it.execute(CREATE_SCHEMA_SQL)
            it.execute(CREATE_OTHER_DATABASE_SQL)
            it.close()
        }
        sql = database.sqlForDefaultDatabaseAndCustomSchema(SCHEMA_NAME).tap {
            it.execute(CREATE_TABLE_SQL)
        }
        sqlForOtherDatabase = database.sqlForCustomDatabaseAndDefaultSchema(OTHER_DATABASE_NAME).tap {
            it.execute(CREATE_TABLE_SQL)
        }
        databaseConfig = database.configuration()
        replicationConfig = ReplicationConfiguration.builder()
                .slotName(REPLICATION_SLOT_NAME)
                .schemaNameToDetectChangesFrom(SCHEMA_NAME)
                .tableNameToDetectChangesFrom(TABLE_NAME)
                .statusIntervalInMillis(1)
                .pollingIntervalInMillis(1)
                .build()
    }

    def setup() {
        consumer = startedConsumer()
    }

    def cleanup() {
        sql.execute(DELETE_ALL_SQL)
        sqlForOtherDatabase.execute(DELETE_ALL_SQL)
        inMemoryPublisher.reset()
        exceptionThrowingPublisher.reset()
        consumer.close()
        sql.execute(DROP_REPLICATION_SLOT, [REPLICATION_SLOT_NAME])
    }

    def cleanupSpec() {
        sql.execute(DROP_SCHEMA_SQL)
    }

    @Unroll
    def "consumes data from inserts only"() {
        when:
        numberOfInserts.times {
            sql.executeInsert(INSERT_SQL, [it, "some data $it" as String])
            sql.executeUpdate(UPDATE_DATA_BY_ID_SQL, ["updated data $it" as String, it])
            sql.execute(DELETE_BY_ID_SQL, it)
        }

        then:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
            walBytesRemainingToConsume() == 0
            inMemoryPublisher.published() == published
        }

        where:
        numberOfInserts || published
        0               || []
        1               || [new Data(id: 0, data: "some data 0")]
        5               || [
                new Data(id: 0, data: "some data 0"),
                new Data(id: 1, data: "some data 1"),
                new Data(id: 2, data: "some data 2"),
                new Data(id: 3, data: "some data 3"),
                new Data(id: 4, data: "some data 4")
        ]
    }

    def "does not miss publishing changed data that failed to be published"() {
        given:
        exceptionThrowingPublisher.willThrowException()
        sql.executeInsert(INSERT_SQL, [2, "stuff 2"])

        expect:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
            exceptionThrowingPublisher.hasThrownException()
        }
        inMemoryPublisher.empty()

        when:
        exceptionThrowingPublisher.willNotThrowException()

        then:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
            walBytesRemainingToConsume() == 0
            inMemoryPublisher.published() == [
                    new Data(id: 2, data: "stuff 2")
            ]
        }
    }

    def "resilient to restart with no data changes missed"() {
        given:
        exceptionThrowingPublisher.willThrowException()
        sql.executeInsert(INSERT_SQL, [1, "stuff"])

        and:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
            exceptionThrowingPublisher.hasThrownException()
        }

        and:
        consumer.close()
        inMemoryPublisher.reset()
        exceptionThrowingPublisher.willNotThrowException()

        when:
        consumer = startedConsumer()

        then:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
            walBytesRemainingToConsume() == 0
            inMemoryPublisher.published() == [
                    new Data(id: 1, data: "stuff")
            ]
        }
    }

    def "executing multiple consumers provides failover"() {
        given:
        def firstConsumer = consumer
        def secondConsumer = startedConsumer()

        and:
        firstConsumer.close()

        expect:
        inMemoryPublisher.empty()

        when:
        sql.executeInsert(INSERT_SQL, [1, "some data 1"])

        then:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
            walBytesRemainingToConsume() == 0
            inMemoryPublisher.published() == [new Data(id: 1, data: "some data 1")]
        }

        cleanup:
        secondConsumer.close()
    }

    def "change data capture messages from other tables in same database and schema are consumed but not published"() {
        given:
        sql.execute(CREATE_OTHER_TABLE_SQL)

        when:
        10.times {
            sql.executeInsert(OTHER_INSERT_SQL, [it, "stuff $it" as String])
        }

        then:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
            walBytesRemainingToConsume() == 0
            inMemoryPublisher.empty()
        }
    }

    def "change data capture messages from other schemas of the same database are consumed but not published"() {
        given:
        sql.execute(CREATE_OTHER_SCHEMA_SQL)
        def sqlForOtherSchema = database.sqlForDefaultDatabaseAndCustomSchema(OTHER_SCHEMA_NAME).tap {
            it.execute(CREATE_TABLE_SQL)
        }

        when:
        10.times {
            sqlForOtherSchema.executeInsert(INSERT_SQL, [it, "stuff $it" as String])
        }

        then:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
            walBytesRemainingToConsume() == 0
            inMemoryPublisher.empty()
        }

        cleanup:
        sqlForOtherSchema.close()
    }

    def "consumer LSN position remains up-to-date (via keepalive messages) during activity in other database"() {
        when:
        10.times {
            sqlForOtherDatabase.executeInsert(INSERT_SQL, [it, "stuff $it" as String])
        }

        then:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
            walBytesRemainingToConsume() == 0
            inMemoryPublisher.empty()
        }
    }

    def "keepalive messages due to other database activity does not advance consumer LSN position while publishing \
         of change data capture message remains unsuccessful"() {
        given:
        exceptionThrowingPublisher.willThrowException()
        sql.executeInsert(INSERT_SQL, [1, "stuff 1"])

        and:
        10.times {
            sqlForOtherDatabase.executeInsert(INSERT_SQL, [it, "stuff $it" as String])
        }

        expect:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
            exceptionThrowingPublisher.hasThrownException()
            walBytesRemainingToConsume() > 0
            inMemoryPublisher.empty()
        }

        when:
        exceptionThrowingPublisher.willNotThrowException()

        then:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
            walBytesRemainingToConsume() == 0
            inMemoryPublisher.published() == [new Data(id: 1, data: "stuff 1")]
        }
    }

    private boolean replicationSlotExists() {
        sql.rows("SELECT * FROM pg_replication_slots WHERE slot_name = ?", [REPLICATION_SLOT_NAME]).size() == 1
    }

    private int walBytesRemainingToConsume() {
        def lsnValues = sql.firstRow(
                """SELECT 
                     pg_current_wal_lsn() AS current_lsn, 
                     (SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = ?)""",
                [REPLICATION_SLOT_NAME]
        )
        def currentLsn = lsnValues.current_lsn
        def consumedLsn = lsnValues.confirmed_flush_lsn
        println "Current LSN: ${currentLsn}, Consumed LSN: ${consumedLsn}"
        def bytesRemaining = sql.firstRow(
                "SELECT pg_wal_lsn_diff(?, ?) AS size_bytes", [currentLsn, consumedLsn]
        ).size_bytes as Long
        println("Bytes remaining to consume: $bytesRemaining")
        bytesRemaining
    }

    private ChangeDataCaptureConsumer startedConsumer() {
        def consumer = new ChangeDataCaptureConsumer(
                exceptionThrowingPublisher,
                replicationConfig,
                { new PostgresReplicationStream(databaseConfig, replicationConfig) }
        ).tap {
            it.start()
        }
        while (!replicationSlotExists()) {
            Thread.sleep(100)
        }
        consumer
    }
}
