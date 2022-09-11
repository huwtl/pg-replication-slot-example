package org.huwtl.pgrepl.consumer

import org.huwtl.pgrepl.db.EmbeddedDatabase
import org.huwtl.pgrepl.publisher.InMemoryPublishedDataStore
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class PostgresDataChangeConsumerTest extends Specification {
    @Shared
    @AutoCleanup
    private final database = new EmbeddedDatabase()

    @AutoCleanup
    private final dataStore = new InMemoryPublishedDataStore<String>()
    private final consumer = new PostgresDataChangeConsumer(dataStore)

    @Unroll
    def "consumes data"() {
        when:
        numberOfPublishes.times { consumer.consume() }

        then:
        dataStore.published() == published

        where:
        numberOfPublishes || published
        0                 || []
        1                 || ["some data"]
        2                 || ["some data", "some data"]
    }
}
