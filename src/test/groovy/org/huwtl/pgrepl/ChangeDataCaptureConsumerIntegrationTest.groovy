package org.huwtl.pgrepl

import groovy.sql.Sql
import org.huwtl.pgrepl.application.services.consumer.ChangeDataCaptureConsumer
import org.huwtl.pgrepl.infrastructure.postgres.EmbeddedPostgresContainer
import org.huwtl.pgrepl.infrastructure.postgres.PostgresReplicationStream
import org.huwtl.pgrepl.application.services.publisher.Data
import org.huwtl.pgrepl.application.services.publisher.ExceptionThrowingPublisher
import org.huwtl.pgrepl.application.services.publisher.InMemoryPublishedDataStore
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
import spock.util.concurrent.PollingConditions

class ChangeDataCaptureConsumerIntegrationTest extends Specification {
    private static final String SCHEMA_NAME = "replication_test"
    private static final String TABLE_NAME = "events"
    private static final String REPLICATION_SLOT_NAME = "replication_test"
    private static final String CREATE_SCHEMA_SQL = "CREATE SCHEMA ${SCHEMA_NAME}"
    private static final String DROP_SCHEMA_SQL = "DROP SCHEMA ${SCHEMA_NAME} CASCADE"
    private static final String CREATE_TABLE_SQL = "CREATE TABLE ${TABLE_NAME}(id INT PRIMARY KEY, data TEXT NOT NULL)"
    private static final String INSERT_SQL = "INSERT INTO ${TABLE_NAME}(id, data) VALUES(?, ?)"
    private static final String UPDATE_DATA_BY_ID_SQL = "UPDATE ${TABLE_NAME} SET data = ? WHERE id = ?"
    private static final String DELETE_BY_ID_SQL = "DELETE FROM ${TABLE_NAME} WHERE id = ?"
    private static final String DELETE_ALL_SQL = "DELETE FROM ${TABLE_NAME}"
    private static final int ASYNC_ASSERTION_TIMEOUT_IN_SECS = 5

    @Shared
    @AutoCleanup
    private Sql sql
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
        database.sql().tap {
            it.execute(CREATE_SCHEMA_SQL)
        }
        sql = database.sqlForSchema(SCHEMA_NAME).tap {
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
        inMemoryPublisher.reset()
        exceptionThrowingPublisher.reset()
        consumer.close()
    }

    def cleanupSpec() {
        sql.execute(DROP_SCHEMA_SQL)
    }

    @Unroll
    def "consumes data from inserts only"() {
        when:
        numberOfInserts.times {
            sql.executeInsert(INSERT_SQL, [it, "some data $it" as String])
            sql.executeInsert(UPDATE_DATA_BY_ID_SQL, ["updated data $it" as String, it])
            sql.executeInsert(DELETE_BY_ID_SQL, it)
        }

        then:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
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
            assert exceptionThrowingPublisher.hasThrownException()
        }

        and:
        consumer.close()
        inMemoryPublisher.reset()
        exceptionThrowingPublisher.willNotThrowException()

        when:
        consumer = startedConsumer()

        then:
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
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
            inMemoryPublisher.published() == [new Data(id: 1, data: "some data 1")]
        }

        cleanup:
        secondConsumer.close()
    }

    private ChangeDataCaptureConsumer startedConsumer() {
        new ChangeDataCaptureConsumer(
                exceptionThrowingPublisher,
                replicationConfig,
                { new PostgresReplicationStream(databaseConfig, replicationConfig) }
        ).tap {
            it.start()
        }
    }
}
