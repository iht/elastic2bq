package dev.herraiz.beam.schemas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonSchemaParser {

  private static final String ROOT_NODE_PATH = "schema";
  private static final String FIELDS_NODE_PATH = "fields";

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonSchemaParser.class);

  public JsonSchemaParser(String schemaAsString) {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = null;
    try {
      root = mapper.readTree(schemaAsString);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      LOGGER.error("Unable to parse the JSON schema provided. Make sure it is a BQ-style schema.");
      System.exit(1);
    }
    JsonNode rootNode = root.path(ROOT_NODE_PATH);
  }

  private static Schema parseSchema(JsonNode rootNode) {
    List<Field> beamFields = new ArrayList<>();
    for (JsonNode childNode : rootNode.path(FIELDS_NODE_PATH)) {
      boolean isRepeated = childNode.path("mode").asText().toUpperCase().equals("REPEATED");

      String name = childNode.path("name").asText();
      String type = childNode.path("type").asText().toUpperCase();

      FieldType beamType;

      if (type.equals("RECORD")) {
        Schema recordSchema = parseSchema(childNode);
        beamType = FieldType.row(recordSchema);
      } else {
        beamType = string2FieldType(type);
      }

      Field field;
      if (isRepeated) {
        field = Field.of(name, FieldType.array(beamType));
      } else {
        field = Field.of(name, beamType);
      }

      beamFields.add(field);
    }

    return Schema.of(beamFields.toArray(Field[]::new));
  }

  private static FieldType string2FieldType(String type) {
    FieldType beamType = switch (type) {
      case "STRING" -> FieldType.STRING;
      case "BYTES" -> FieldType.BYTES;
      case "NUMERIC", "BIGNUMERIC" -> FieldType.DECIMAL;
      case "BOOL" -> FieldType.BOOLEAN;
      case "DATETIME" -> FieldType.DATETIME;
      case "INT64", "INT" -> FieldType.INT64;
      case "FLOAT64" -> FieldType.DOUBLE;
      default -> null;
    };

    assert beamType != null;
    return beamType;
  }

}
