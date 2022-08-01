package dev.herraiz.beam.schemas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;

public class JsonSchemaParser {

  private Map<String, JsonNode> elementsMap;

  public JsonSchemaParser(String schemaAsString) throws JsonProcessingException {
    this.elementsMap = new HashMap<>();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readTree(schemaAsString);
  }

  private static void parseSchema(JsonNode rootNode) {

  }
}
