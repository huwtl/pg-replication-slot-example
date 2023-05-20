package org.huwtl.pgrepl.consumer

import org.huwtl.pgrepl.ReplicationConfiguration
import org.huwtl.pgrepl.publisher.Data
import org.huwtl.pgrepl.publisher.ExceptionThrowingPublisher
import org.huwtl.pgrepl.publisher.InMemoryPublishedDataStore
import spock.lang.AutoCleanup
import spock.lang.Specification
import spock.lang.Unroll
import spock.util.concurrent.PollingConditions

import static org.huwtl.pgrepl.consumer.ReplicationStreamMessage.NoMessage

class DataChangeConsumerTest extends Specification {
    private static final SCHEMA_OF_INTEREST = "test_schema"
    private static final TABLE_OF_INTEREST = "test_table"
    private static final POLLING_INTERVAL_IN_MILLIS = 100 as long

    private def inMemoryPublisher = new InMemoryPublishedDataStore()
    private def exceptionThrowingPublisher = ExceptionThrowingPublisher.willNotThrowException(inMemoryPublisher)
    private def replicationStream = new DatabaseAgnosticReplicationStream()
    private def replicationStreamProvider = { replicationStream } as ReplicationStreamProvider
    private def executorService = new SynchronousExecutionService()
    private def delayService = new FakeDelayService()

    @AutoCleanup
    private def consumer = new DataChangeConsumer(
            exceptionThrowingPublisher,
            ReplicationConfiguration.builder()
                    .slotName("any")
                    .schemaNameToDetectChangesFrom(SCHEMA_OF_INTEREST)
                    .tableNameToDetectChangesFrom(TABLE_OF_INTEREST)
                    .pollingIntervalInMillis(POLLING_INTERVAL_IN_MILLIS)
                    .build(),
            replicationStreamProvider,
            executorService,
            delayService
    )

    @Unroll
    def "consumes and publishes change data capture messages specific for database table"() {
        given:
        consumerStarted()

        expect:
        inMemoryPublisher.empty()

        when:
        replicationStream.nextMessagesToReturn(actualDataOfMessages.collect {
            new DatabaseAgnosticChangeDataCaptureMessage(schema: schema, table: table, data: it)
        })

        then:
        new PollingConditions(timeout: 5).eventually {
            inMemoryPublisher.published() == expectedDataPublished
        }
        delayService.noDelaysApplied()

        where:
        schema               | table               | actualDataOfMessages                               || expectedDataPublished
        SCHEMA_OF_INTEREST   | TABLE_OF_INTEREST   | [message(data(stuff: "a"))]                        || [data(stuff: "a")]
        SCHEMA_OF_INTEREST   | TABLE_OF_INTEREST   | [message(data(val: "1"), data(val: "2"))]          || [data(val: "1"), data(val: "2")]
        SCHEMA_OF_INTEREST   | TABLE_OF_INTEREST   | [message(data(val: "1")), message(data(val: "2"))] || [data(val: "1"), data(val: "2")]
        SCHEMA_OF_INTEREST   | TABLE_OF_INTEREST   | [message()]                                        || []
        SCHEMA_OF_INTEREST   | TABLE_OF_INTEREST   | []                                                 || []
        "schema no interest" | TABLE_OF_INTEREST   | [message(data(stuff: "a"))]                        || []
        SCHEMA_OF_INTEREST   | "table no interest" | [message(data(stuff: "a"))]                        || []
    }

    def "applies polling delay and then continues consuming change data capture messages after error"() {
        given:
        def changeDataCaptureMessage = new DatabaseAgnosticChangeDataCaptureMessage(
                schema: SCHEMA_OF_INTEREST,
                table: TABLE_OF_INTEREST,
                data: [data(stuff: "a")]
        )
        def delaysAppliedAfterError = [POLLING_INTERVAL_IN_MILLIS]

        and:
        consumerStarted()
        exceptionThrowingPublisher.willThrowException()
        replicationStream.nextMessagesToReturn([changeDataCaptureMessage])

        expect:
        new PollingConditions(timeout: 5).eventually {
            exceptionThrowingPublisher.hasThrownException()
            inMemoryPublisher.empty()
            delayService.delaysApplied() == delaysAppliedAfterError
        }

        when:
        exceptionThrowingPublisher.willNotThrowException()
        replicationStream.nextMessagesToReturn([changeDataCaptureMessage])

        then:
        new PollingConditions(timeout: 5).eventually {
            inMemoryPublisher.published() == changeDataCaptureMessage.data
        }

        and:
        delayService.delaysApplied() == delaysAppliedAfterError
    }

    @Unroll
    def "applies a polling delay every time no changes found during poll"() {
        given:
        consumerStarted()

        expect:
        inMemoryPublisher.empty()
        delayService.noDelaysApplied()
        delayService.delaysApplied().empty

        when:
        replicationStream.nextMessagesToReturn(
                [new NoMessage()] * numberOfTimesNoMessagesWereConsumed
        )

        then:
        new PollingConditions(timeout: 5).eventually {
            !delayService.noDelaysApplied()
            delayService.delaysApplied() == [POLLING_INTERVAL_IN_MILLIS] * numberOfTimesNoMessagesWereConsumed
        }

        where:
        numberOfTimesNoMessagesWereConsumed << [1, 2, 3]
    }

    def "does not apply polling delay if interrupted"() {
        given:
        consumerStarted()
        delayService.willBeInterrupted()

        expect:
        inMemoryPublisher.empty()
        delayService.noDelaysApplied()
        delayService.delaysApplied().empty
        delayService.numberOfTimesInterrupted() == 0

        when:
        replicationStream.nextMessagesToReturn([
                new NoMessage()
        ])

        then:
        new PollingConditions(timeout: 5).eventually {
            delayService.numberOfTimesInterrupted() == 1
        }
        inMemoryPublisher.empty()
        delayService.noDelaysApplied()
        delayService.delaysApplied().empty
    }

    def "shutdown gracefully"() {
        given:
        consumerStarted()

        and:
        executorService.willShutdownGracefully()

        expect:
        !executorService.isShutdown()

        when:
        consumer.close()

        then:
        executorService.isShutdown()
        executorService.shutdownGracefully()
        !executorService.failedToShutdownGracefully()
    }

    def "fails to shutdown gracefully"() {
        given:
        consumerStarted()

        and:
        executorService.willFailToShutdownGracefully()

        expect:
        !executorService.isShutdown()

        when:
        consumer.close()

        then:
        executorService.isShutdown()
        executorService.failedToShutdownGracefully()
        !executorService.shutdownGracefully()
    }

    def "fails to shutdown gracefully when throws interrupted exception"() {
        given:
        consumerStarted()

        and:
        executorService.willBeInterruptedWhenAwaitingTermination()

        expect:
        !executorService.isShutdown()

        when:
        consumer.close()

        then:
        executorService.isShutdown()
        executorService.failedToShutdownGracefully()
        !executorService.shutdownGracefully()
    }

    private Thread consumerStarted() {
        Thread.start { consumer.start() }
    }

    private static Data data(Map data) {
        new Data(data)
    }

    private static List<Data> message(Data... data) {
        data
    }
}
