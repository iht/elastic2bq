package dev.herraiz.beam.schemas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonSchemaParser {

  private static final String ROOT_NODE_NAME = "schema";
  private static final String FIELDS_NODE_NAME = "fields";
  private static final String REPEATED_MODE = "REPEATED";

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonSchemaParser.class);

  private final Map<String, JsonNode> jsonFieldsMap;

  public JsonSchemaParser(String schemaAsString) {

    this.jsonFieldsMap = new HashMap<>();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = null;
    try {
      root = mapper.readTree(schemaAsString);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      LOGGER.error("Unable to parse the JSON schema provided. Make sure it is a BQ-style schema.");
      System.exit(1);
    }
    JsonNode rootNode = root.path(ROOT_NODE_NAME);
  }

  private static void parseSchema(JsonNode rootNode) {
    for (JsonNode childField : rootNode.path(FIELDS_NODE_NAME)) {
      if (childField.path("mode").asText().equals("REPEATED")) {

      }
    }
  }

  public Map<String, JsonNode> getJsonFieldsMap() {
    return jsonFieldsMap;
  }
}
