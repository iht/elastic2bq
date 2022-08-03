package dev.herraiz.beam.pipelines;

import dev.herraiz.beam.options.Elastic2BQOptions;
import dev.herraiz.beam.schemas.JsonSchemaParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.JsonToRow.ParseResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class Elastic2BQ {

  public static void main(String[] args) throws Exception {
    Elastic2BQOptions options = PipelineOptionsFactory.fromArgs(args).as(Elastic2BQOptions.class);

    Pipeline p = Pipeline.create(options);

    String[] host = {options.getElasticHost()};

    String schemaStr = options.getSchema();
    Schema schema = JsonSchemaParser.bqJson2BeamSchema(schemaStr);

    PCollection<String> jsonStrings = p.apply("Read from Elastic",
        ElasticsearchIO.read().withConnectionConfiguration(
            ConnectionConfiguration.create(
                host,
                options.getElasticIndex(),
                options.getElasticType())
        ));

    ParseResult parsedJson = jsonStrings.apply("Parse JSON",
        JsonToRow.withExceptionReporting(schema));

    // This will have our schema
    PCollection<Row> correct = parsedJson.getResults();
    // And the failures will have ERROR_ROW_SCHEMA
    PCollection<Row> failed = parsedJson.getFailedToParseLines();

    //correct.apply("Write corret to BigQuery", BigQueryIO.write())
  }
}
