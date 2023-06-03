package org.huwtl.pgrepl.infrastructure.wal2json

import org.huwtl.pgrepl.application.services.publisher.Data
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class ReplicationSlotMessageDtoTest extends Specification {
    @Shared
    private def schema = "schema1"
    @Shared
    private def table = "table1"

    @Unroll
    def "retrieves data inserted into a given table of a schema (updates and deletes are ignored)"() {
        given:
        def dto = new ReplicationSlotMessageDto(anyXid(), changes as List<ChangeDataCaptureDto>)

        expect:
        dto.capturedDataFromInserts(schema, table) == filteredChanges

        where:
        changes                                                                          || filteredChanges
        [insertChange(schema, table, ["a": "1"])]                                        || [new Data("a": "1")]
        [insertChange(schema.toUpperCase(), table.toUpperCase(), ["a": "1"])]            || [new Data("a": "1")]
        [insertChange(schema, table, ["a": "1", "b": 2])]                                || [new Data("a": "1", "b": 2)]
        [insertChange(schema, table, [:])]                                               || [new Data([:])]
        [insertChange(schema, table, ["a": "1"]), changeToIgnore(schema, table)]         || [new Data("a": "1")]
        [changeToIgnore(schema, table), insertChange(schema, table, ["a": "1"])]         || [new Data("a": "1")]
        [insertChange(schema, table, ["a": "1"]), insertChange(schema, table, ["b": 1])] || [new Data("a": "1"), new Data("b": 1)]
        [insertChange(schema, table, ["b": 1]), insertChange(schema, table, ["a": "1"])] || [new Data("b": 1), new Data("a": "1")]
        [changeToIgnore(schema, table)]                                                  || []
        [insertChange(schema, "table-non-matching", ["a": "1"])]                         || []
        [insertChange("schema-non-matching", table, ["a": "1"])]                         || []
        [changeToIgnore("schema-non-matching", table)]                                   || []
        [changeToIgnore("schema-non-matching", "table-non-matching")]                    || []
        []                                                                               || []
    }

    private static ChangeDataCaptureDto.ChangeToIgnore changeToIgnore(String schema, String table) {
        new ChangeDataCaptureDto.ChangeToIgnore("some-delete-or-update-type", schema, table)
    }

    private static ChangeDataCaptureDto.InsertChange insertChange(String schema,
                                                                  String table,
                                                                  Map<String, Object> data) {
        def dataEntries = data.entrySet() as List
        new ChangeDataCaptureDto.InsertChange(
                "insert",
                schema,
                table,
                dataEntries.collect { it.key },
                dataEntries.collect { it.value }
        )
    }

    private static long anyXid() {
        666
    }
}
