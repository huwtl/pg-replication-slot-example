package org.huwtl.pgrepl.application.services.publisher

import spock.lang.Specification
import spock.lang.Unroll

class CountingPublisherTest extends Specification {
    private def publisher = new CountingPublisher()

    @Unroll
    def "counts number of publishes"() {
        given:
        numberOfPublishes.times {
            publisher.publish(new Data([:]))
        }

        expect:
        publisher.numberOfPublishes() == numberOfPublishes

        where:
        numberOfPublishes << [0, 1, 10]
    }
}
