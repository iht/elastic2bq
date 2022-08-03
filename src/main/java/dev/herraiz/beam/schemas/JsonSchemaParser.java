package dev.herraiz.beam.schemas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;


/**
 * This class allows you to transform a BigQuery JSON string into a Beam schema.
 */
public class JsonSchemaParser {

  private static final String ROOT_NODE_PATH = "schema";
  private static final String FIELDS_NODE_PATH = "fields";

  /**
   * Transform a BigQuery JSON schema into a Beam schema
   *
   * @param schemaAsString A string with the full BigQuery schema encoded as JSON
   * @return A Beam schema that can be used with Row and other Beam classes.
   * @throws JsonProcessingException If the string cannot be parsed as JSON.
   */
  public static Schema bqJson2BeamSchema(String schemaAsString) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode topNode = mapper.readTree(schemaAsString);
    JsonNode schemaRootNode = topNode.path(ROOT_NODE_PATH);
    if (schemaRootNode.isMissingNode()) {
      throw new Exception(
          "Is this a BQ schema? The given schema must have a top node of name " + ROOT_NODE_PATH);
    }

    return parseSchema(schemaRootNode);
  }

  private static Schema parseSchema(JsonNode rootNode) throws Exception {
    List<Field> beamFields = new ArrayList<>();
    JsonNode children = rootNode.path(FIELDS_NODE_PATH);
    if (children.isMissingNode()) {
      throw new Exception("Is this a BQ schema? The schema field must have another field of name "
          + FIELDS_NODE_PATH);
    }

    for (JsonNode childNode : children) {
      String mode = childNode.path("mode").asText().toUpperCase();
      boolean isRepeated = mode.equals("REPEATED");
      boolean isNullable = mode.equals("NULLABLE");

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
      } else if (isNullable) {
        field = Field.nullable(name, beamType);
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
