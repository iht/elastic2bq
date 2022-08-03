package dev.herraiz.beam.schemas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class JsonSchemaParser {

  private static final String ROOT_NODE_PATH = "schema";
  private static final String FIELDS_NODE_PATH = "fields";

  public static Schema bqJson2BeamSchema(String schemaAsString) throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode topNode = mapper.readTree(schemaAsString);
    JsonNode schemaRootNode = topNode.path(ROOT_NODE_PATH);
    return parseSchema(schemaRootNode);
  }

  private static Schema parseSchema(JsonNode rootNode) {
    List<Field> beamFields = new ArrayList<>();
    for (JsonNode childNode : rootNode.path(FIELDS_NODE_PATH)) {
      boolean isRepeated = childNode.path("mode").asText().equalsIgnoreCase("REPEATED");

      String name = childNode.path("name").asText();
      String type = childNode.path("type").asText().toUpperCase();

      FieldType beamType;

      if (type.equals("RECORD")) {
        Schema recordSchema = parseSchema(childNode);
        beamType = FieldType.row(recordSchema);
      } else {
        beamType = string2FieldType(type);
      }

      assert beamType != null;

      Field field;
      if (isRepeated) {
        field = Field.of(name, FieldType.array(beamType));
      } else {
        field = Field.of(name, beamType);
      }

      assert field != null;

      beamFields.add(field);
    }

    return Schema.of(beamFields.toArray(Field[]::new));
  }

  private static FieldType string2FieldType(String type) {
    FieldType beamType = switch (type) {
      case "STRING" -> FieldType.STRING;
      case "BYTES" -> FieldType.BYTES;
      case "NUMERIC", "BIGNUMERIC" -> FieldType.DECIMAL;
      case "BOOL", "BOOLEAN" -> FieldType.BOOLEAN;
      case "DATETIME" -> FieldType.DATETIME;
      case "INT64", "INT", "INTEGER" -> FieldType.INT64;
      case "FLOAT64" -> FieldType.DOUBLE;
      default -> null;
    };

    assert beamType != null;
    return beamType;
  }
}
