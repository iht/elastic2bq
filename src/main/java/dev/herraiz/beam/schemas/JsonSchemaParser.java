package dev.herraiz.beam.schemas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;


/**
 * Transform a BigQuery JSON string into a Beam schema.
 */
public class JsonSchemaParser {

  private static final String ROOT_NODE_PATH = "schema";

  public static Schema bqJson2BeamSchema(String schemaAsString) throws Exception {
    JsonFactory defaultJsonFactory = Utils.getDefaultJsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode topNode = mapper.readTree(schemaAsString);
    JsonNode schemaRootNode = topNode.path(ROOT_NODE_PATH);
    if (schemaRootNode.isMissingNode()) {
      throw new Exception(
          "Is this a BQ schema? The given schema must have a top node of name " + ROOT_NODE_PATH);
    }

    TableSchema tableSchema =
        defaultJsonFactory.fromString(schemaRootNode.toString(), TableSchema.class);

    return BigQueryUtils.fromTableSchema(tableSchema);
  }
}