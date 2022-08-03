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
    // We assume the schema is a BQ JSON schema, with a schema field, containing an array of
    // name fields.
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

      assert !mode.isEmpty();
      assert !name.isEmpty();
      assert !type.isEmpty();

      FieldType beamType;
      if (type.equals("RECORD")) {
        // If this is a nested record, we recursively parse the nested record
        Schema recordSchema = parseSchema(childNode);
        beamType = FieldType.row(recordSchema);  // The type will be "row"
      } else {
        // If it is not a record, then it must be one of the basic types of BQ
        beamType = string2FieldType(type);
      }

      assert beamType != null;

      Field field;
      // In BQ schemas, REPEATED and NULLABLE cannot happen at the same time
      if (isRepeated) {
        field = Field.of(name, FieldType.array(beamType));
      } else if (isNullable) {
        field = Field.nullable(name, beamType);
      } else {
        // If the mode was not NULLABLE nor REPEATED, it had to be REQUIRED
        field = Field.of(name, beamType);
      }

      assert field != null;

      beamFields.add(field);
    }

    return Schema.of(beamFields.toArray(Field[]::new));
  }

  private static FieldType string2FieldType(String type) {
    // We map the BQ types to the corresponding Beam types.
    // FIXME: this mapping is not yet exhaustive, some types are missing
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

    // FIXME: we assume we can map all BQ types to a Beam type
    // There are some BQ types (GEOGRAPHY, JSON, etc) that cannot be mapped to any Beam type.
    // We should add some exception or warning about those columns (e.g. if we decide to ignore
    // those columns).
    assert beamType != null;
    return beamType;
  }
}
