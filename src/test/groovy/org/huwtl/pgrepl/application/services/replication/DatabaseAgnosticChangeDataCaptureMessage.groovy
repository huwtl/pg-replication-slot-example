package org.huwtl.pgrepl.application.services.replication

import groovy.transform.Canonical
import org.huwtl.pgrepl.application.services.publisher.Data

import static org.huwtl.pgrepl.application.services.replication.ReplicationStreamMessage.ChangeDataCaptureMessage

@Canonical
class DatabaseAgnosticChangeDataCaptureMessage implements ChangeDataCaptureMessage {
    String schema
    String table
    List<Data> data

    @Override
    List<Data> capturedDataFromInserts(String schema, String table) {
        (this.schema == schema && this.table == table) ? data : []
    }
}
