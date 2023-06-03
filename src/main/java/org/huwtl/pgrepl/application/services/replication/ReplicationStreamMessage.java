package org.huwtl.pgrepl.application.services.replication;

import org.huwtl.pgrepl.application.services.publisher.Data;

import java.util.List;

public interface ReplicationStreamMessage {
    interface ChangeDataCaptureMessage extends ReplicationStreamMessage {
        List<Data> capturedDataFromInserts(String schema, String table);
    }

    record NoMessage() implements ReplicationStreamMessage {
    }
}
