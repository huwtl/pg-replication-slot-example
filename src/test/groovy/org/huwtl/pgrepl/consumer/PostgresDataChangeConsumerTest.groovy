package org.huwtl.pgrepl.consumer

import groovy.sql.Sql
import org.huwtl.pgrepl.DatabaseConfiguration
import org.huwtl.pgrepl.ReplicationConfiguration
import org.huwtl.pgrepl.db.EmbeddedPostgresContainer
import org.huwtl.pgrepl.publisher.Data
import org.huwtl.pgrepl.publisher.ExceptionThrowingPublisher
import org.huwtl.pgrepl.publisher.InMemoryPublishedDataStore
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
import spock.util.concurrent.PollingConditions

class PostgresDataChangeConsumerTest extends Specification {
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
    @AutoCleanup
    private PostgresDataChangeConsumer consumer
    @Shared
    private InMemoryPublishedDataStore inMemoryPublisher
    @Shared
    private ExceptionThrowingPublisher exceptionThrowingPublisher
    @Shared
    private DatabaseConfiguration databaseConfig
    @Shared
    private ReplicationConfiguration replicationConfig

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
        consumer = startedConsumer()
    }

    def cleanup() {
        sql.execute(DELETE_ALL_SQL)
        inMemoryPublisher.reset()
        exceptionThrowingPublisher.reset()
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
        sql.executeInsert(INSERT_SQL, [1, "stuff"])
        new PollingConditions(timeout: ASYNC_ASSERTION_TIMEOUT_IN_SECS).eventually {
            assert inMemoryPublisher.published() == [
                    new Data(id: 1, data: "stuff")
            ]
        }
        inMemoryPublisher.reset()

        and:
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

    private PostgresDataChangeConsumer startedConsumer() {
        new PostgresDataChangeConsumer(exceptionThrowingPublisher, databaseConfig, replicationConfig).tap {
            it.start()
        }
    }
}
