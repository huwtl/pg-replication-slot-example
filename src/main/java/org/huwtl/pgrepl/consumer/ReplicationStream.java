package org.huwtl.pgrepl.consumer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.function.Consumer;

public interface ReplicationStream extends AutoCloseable {
    void processNextChangeDataCaptureMessage(
            Consumer<ReplicationStreamMessage.ChangeDataCaptureMessage> onChangeDataCaptureMessage,
            Consumer<ReplicationStreamMessage.NoMessage> onNoMessage) throws SQLException, IOException;
}
