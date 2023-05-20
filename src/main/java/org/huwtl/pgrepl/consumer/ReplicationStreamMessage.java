package org.huwtl.pgrepl.consumer;

import org.huwtl.pgrepl.publisher.Data;

import java.util.List;

interface ReplicationStreamMessage {
    interface ChangeDataCaptureMessage extends ReplicationStreamMessage {
        List<Data> filterInsertsBySchemaAndTable(String schema, String table);
    }

    record NoMessage() implements ReplicationStreamMessage {
    }
}
