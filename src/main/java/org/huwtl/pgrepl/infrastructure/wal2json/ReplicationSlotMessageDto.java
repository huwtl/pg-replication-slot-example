package org.huwtl.pgrepl.infrastructure.wal2json;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.huwtl.pgrepl.application.services.publisher.Data;
import org.huwtl.pgrepl.application.services.replication.ReplicationStreamMessage;

import java.util.List;

public record ReplicationSlotMessageDto(
        @JsonProperty(required = true)
        long xid,
        @JsonProperty(value = "change", required = true)
        List<ChangeDataCaptureDto> changes) implements ReplicationStreamMessage.ChangeDataCaptureMessage {
    @Override
    public List<Data> capturedDataFromInserts(String schema, String table) {
        return changes.stream()
                .filter(ChangeDataCaptureDto::insertTypeChange)
                .filter(change -> change.fromSchema(schema))
                .filter(change -> change.fromTable(table))
                .map(ChangeDataCaptureDto::rowData)
                .toList();
    }
}
