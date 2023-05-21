package org.huwtl.pgrepl.infrastructure.postgres;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.huwtl.pgrepl.application.services.publisher.Data;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static org.huwtl.pgrepl.infrastructure.postgres.ChangeDataCaptureDto.ChangeToIgnore;
import static org.huwtl.pgrepl.infrastructure.postgres.ChangeDataCaptureDto.InsertChange;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "kind",
        visible = true,
        defaultImpl = ChangeToIgnore.class
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = InsertChange.class, name = InsertChange.TYPE),
})
interface ChangeDataCaptureDto {
    String type();

    String schema();

    String table();

    List<String> columnNames();

    List<Object> columnValues();

    default boolean insertTypeChange() {
        return type().equalsIgnoreCase(InsertChange.TYPE);
    }

    default boolean fromSchema(String schema) {
        return schema().equalsIgnoreCase(schema);
    }

    default boolean fromTable(String tableName) {
        return table().equalsIgnoreCase(tableName);
    }

    default Data rowData() {
        return new Data(
                range(0, columnNames().size())
                        .mapToObj(index -> Map.entry(columnNames().get(index), columnValues().get(index)))
                        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
    }

    record InsertChange(
            @JsonProperty(value = "kind", required = true)
            String type,
            @JsonProperty(required = true)
            String schema,
            @JsonProperty(required = true)
            String table,
            @JsonProperty(value = "columnvalues", required = true)
            List<Object> columnValues,
            @JsonProperty(value = "columnnames", required = true)
            List<String> columnNames) implements ChangeDataCaptureDto {
        static final String TYPE = "insert";
    }

    record ChangeToIgnore(
            @JsonProperty(value = "kind", required = true)
            String type,
            @JsonProperty(required = true)
            String schema,
            @JsonProperty(required = true)
            String table) implements ChangeDataCaptureDto {
        @Override
        public List<String> columnNames() {
            return List.of();
        }

        @Override
        public List<Object> columnValues() {
            return List.of();
        }
    }
}
