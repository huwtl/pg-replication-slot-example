package org.huwtl.pgrepl.consumer

import groovy.sql.Sql
import org.huwtl.pgrepl.ReplicationConfiguration
import org.huwtl.pgrepl.db.EmbeddedPostgresContainer
import org.huwtl.pgrepl.publisher.Data
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
    private InMemoryPublishedDataStore publisher

    def setupSpec() {
        database = new EmbeddedPostgresContainer()
        publisher = new InMemoryPublishedDataStore()
        def databaseConfig = database.configuration()
        def replicationConfig = ReplicationConfiguration.builder()
                .slotName(REPLICATION_SLOT_NAME)
                .schemaNameToDetectChangesFrom(SCHEMA_NAME)
                .tableNameToDetectChangesFrom(TABLE_NAME)
                .build()
        consumer = new PostgresDataChangeConsumer(publisher, databaseConfig, replicationConfig)
        database.sql().tap {
            it.execute(CREATE_SCHEMA_SQL)
        }
        sql = database.sqlForSchema(SCHEMA_NAME).tap {
            it.execute(CREATE_TABLE_SQL)
        }
    }

    def cleanup() {
        sql.execute(DELETE_ALL_SQL)
        publisher.reset()
    }

    def cleanupSpec() {
        sql.execute(DROP_SCHEMA_SQL)
    }

    @Unroll
    def "consumes data"() {
        given:
        numberOfInserts.times {
            sql.executeInsert(INSERT_SQL, [it, "some data $it" as String])
            sql.executeInsert(UPDATE_DATA_BY_ID_SQL, ["updated data $it" as String, it])
            sql.executeInsert(DELETE_BY_ID_SQL, it)
        }

        when:
        consumer.start()

        then:
        new PollingConditions(timeout: 10).eventually {
            publisher.published() == published
        }

        where:
        numberOfInserts || published
        0               || []
        1               || [new Data(id: 0, data: "some data 0")]
        5               || [
                new Data([id: 0, data: "some data 0"]),
                new Data([id: 1, data: "some data 1"]),
                new Data([id: 2, data: "some data 2"]),
                new Data([id: 3, data: "some data 3"]),
                new Data([id: 4, data: "some data 4"])
        ]
    }
}
