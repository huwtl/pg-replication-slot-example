package org.huwtl.pgrepl.consumer

import groovy.transform.Canonical
import org.huwtl.pgrepl.publisher.Data

import static org.huwtl.pgrepl.consumer.ReplicationStreamMessage.ChangeDataCaptureMessage

@Canonical
class DatabaseAgnosticChangeDataCaptureMessage implements ChangeDataCaptureMessage {
    String schema
    String table
    List<Data> data

    @Override
    List<Data> filterInsertsBySchemaAndTable(String schema, String table) {
        (this.schema == schema && this.table == table) ? data : []
    }
}
