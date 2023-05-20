package org.huwtl.pgrepl.consumer

import java.sql.SQLException
import java.util.function.Consumer

import static org.huwtl.pgrepl.consumer.ReplicationStreamMessage.ChangeDataCaptureMessage
import static org.huwtl.pgrepl.consumer.ReplicationStreamMessage.NoMessage

class DatabaseAgnosticReplicationStream implements ReplicationStream {
    private final List<ReplicationStreamMessage> nextMessages = []

    @Override
    void processNextChangeDataCaptureMessage(
            Consumer<ChangeDataCaptureMessage> onChangeDataCaptureMessage,
            Consumer<NoMessage> onNoMessage) throws SQLException, IOException {

        if (!nextMessages.empty) {
            def nextMessage = nextMessages.pop()
            switch (nextMessage) {
                case ChangeDataCaptureMessage:
                    onChangeDataCaptureMessage.accept(nextMessage)
                    break
                case NoMessage:
                    onNoMessage.accept(nextMessage)
                    break
            }
        }
    }

    @Override
    void close() throws Exception {
    }

    void nextMessagesToReturn(List<ReplicationStreamMessage> messages) {
        nextMessages.addAll(messages)
    }
}
