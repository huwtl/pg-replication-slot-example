package org.huwtl.pgrepl.consumer;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.huwtl.pgrepl.publisher.Data;

import java.util.List;

record SlotMessage(
        @JsonProperty(required = true)
        long xid,
        @JsonProperty(value = "change", required = true)
        List<Change> changes) {
    List<Data> filterInsertsBySchemaAndTable(String schema, String table) {
        return changes.stream()
                .filter(Change::insertTypeChange)
                .filter(change -> change.fromSchema(schema))
                .filter(change -> change.fromTable(table))
                .map(Change::rowData)
                .toList();
    }
}
