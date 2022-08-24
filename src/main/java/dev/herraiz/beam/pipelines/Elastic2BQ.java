package dev.herraiz.beam.pipelines;

import com.google.api.services.bigquery.model.TableReference;
import dev.herraiz.beam.options.Elastic2BQOptions;
import dev.herraiz.beam.schemas.JsonSchemaParser;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.JsonToRow.ParseResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class Elastic2BQ {

  public static void main(String[] args) throws Exception {
    // Parse and set options
    Elastic2BQOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Elastic2BQOptions.class);

    runPipeline(options);
  }

  private static void runPipeline(Elastic2BQOptions options) throws Exception {
    // BigQuery options
    String bigQueryProject =
        Optional.ofNullable(options.getBigQueryProject())
            .orElse(options.as(DataflowPipelineOptions.class).getProject());

    String bigQueryDataset = options.getBigQueryDataset();
    String bigQueryTable = options.getBigQueryTable();
    String errorsProject =
        Optional.ofNullable(options.getBigQueryErrorsProject()).orElse(bigQueryProject);
    String errorsDataset =
        Optional.ofNullable(options.getBigQueryErrorsDataset()).orElse(bigQueryDataset);
    String errorsTable = options.getBigQueryErrorsTable();

    TableReference correctTableRef =
        new TableReference()
            .setProjectId(bigQueryProject)
            .setDatasetId(bigQueryDataset)
            .setTableId(bigQueryTable);

    TableReference errorsTableRef =
        new TableReference()
            .setProjectId(errorsProject)
            .setDatasetId(errorsDataset)
            .setTableId(errorsTable);

    // Elastic options
    String[] host = {options.getElasticHost()};

    // Schema options
    String schemaLocation = options.getSchema();
    String schemaStr = readSchemaFile(schemaLocation);
    Schema schema = JsonSchemaParser.bqJson2BeamSchema(schemaStr);

    Pipeline p = Pipeline.create(options);

    // Read data from Elastic as JSON strings
    PCollection<String> jsonStrings =
        p.apply(
            "Read from Elastic",
            ElasticsearchIO.read()
                .withConnectionConfiguration(
                    ConnectionConfiguration.create(
                        host, options.getElasticIndex(), options.getElasticType())));

    // Parse JSON strings to Beam Row
    ParseResult parsedJson =
        jsonStrings.apply("Parse JSON", JsonToRow.withExceptionReporting(schema));

    // This will have our schema
    PCollection<Row> correct = parsedJson.getResults();
    // And the failures will have ERROR_ROW_SCHEMA
    PCollection<Row> failed = parsedJson.getFailedToParseLines();

    // Write to BigQuery
    correct.apply("Write correct to BQ", bqWriteForTable(correctTableRef));
    failed.apply("Write failures to BQ", bqWriteForTable(errorsTableRef));
  }

  private static Write<Row> bqWriteForTable(TableReference table) {
    return BigQueryIO.<Row>write()
        .to(table)
        .optimizedWrites()
        .withAutoSharding()
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        .withMethod(Write.Method.FILE_LOADS)
        .useBeamSchema();
  }

  private static String readSchemaFile(String schemaLocation) throws IOException {
    ResourceId schemaResource = FileSystems.matchNewResource(schemaLocation, false);
    File tempFile = File.createTempFile("schema", ".json");
    ResourceId localTempFileDestination =
        FileSystems.matchNewResource("file://" + tempFile.getAbsolutePath(), false);
    FileSystems.copy(List.of(schemaResource), List.of(localTempFileDestination));

    String schemaStr = Files.readString(Path.of(tempFile.getAbsolutePath()));

    assert tempFile.delete();

    return schemaStr;
  }
}
