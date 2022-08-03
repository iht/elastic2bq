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
  private static final String REPEATED_MODE = "REPEATED";


  private static final Logger LOGGER = LoggerFactory.getLogger(JsonSchemaParser.class);

  private final Map<String, Field> beamFields = new HashMap<>();

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

  private Schema parseSchema(JsonNode rootNode) {
    List<Field> beamFields = new ArrayList<>();
    for (JsonNode childNode : rootNode.path(FIELDS_NODE_PATH)) {
      boolean isRepated = childNode.path("mode").asText().equals(REPEATED_MODE);

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
      if (isRepated) {
        field = Field.of(name, FieldType.array(beamType));
      } else {
        field = Field.of(name, beamType);
      }

      beamFields.add(field);
    }

    Schema schema = Schema.of(beamFields.toArray(new Field[0]));

    return schema;
  }

  private FieldType string2FieldType(String type) {
    FieldType beamType = null;

    if (type.equals("STRING")) {
      beamType = FieldType.STRING;
    } else if (type.equals("BYTES")) {
      beamType = FieldType.BYTES;
    } else if (type.equals("NUMERIC") || type.equals("BIGNUMERIC")) {
      beamType = FieldType.DECIMAL;
    } else if (type.equals("BOOL")) {
      beamType = FieldType.BOOLEAN;
    } else if (type.equals("DATETIME")) {
      beamType = FieldType.DATETIME;
    } else if (type.equals("INT64") || type.equals("INT")) {
      beamType = FieldType.INT64;
    } else if (type.equals("FLOAT64")) {
      beamType = FieldType.DOUBLE;
    }

    assert beamType != null;
    return beamType;
  }

}
