/*
 * Copyright 2022 Israel Herraiz.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package dev.herraiz.beam.pipelines;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import dev.herraiz.beam.options.Elastic2BQOptions;
import dev.herraiz.beam.parser.Json2RowWithSanitization;
import dev.herraiz.beam.schemas.JsonSchemaParser;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Read;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow.JsonToRowWithErrFn;
import org.apache.beam.sdk.transforms.JsonToRow.ParseResult;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Elastic2BQ {

    private static final Logger logger = LoggerFactory.getLogger(Elastic2BQ.class);

    public static void main(String[] args) throws Exception {
        // Parse and set options
        Elastic2BQOptions options =
                PipelineOptionsFactory.fromArgs(args).as(Elastic2BQOptions.class);

        runPipeline(options);
    }

    private static void runPipeline(Elastic2BQOptions options) throws Exception {

        // BigQuery options
        String gcpProject = options.as(DataflowPipelineOptions.class).getProject();
        String bigQueryProject =
                Optional.ofNullable(options.getBigQueryProject()).orElse(gcpProject);

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
        String elasticIndex = options.getElasticIndex();
        String query = options.getQuery();

        // Register options before trying to grab the schema file from remote storage
        Pipeline p = Pipeline.create(options);

        // Schema options
        String schemaLocation = options.getSchema();
        String schemaStr = readSchemaFile(schemaLocation, gcpProject);
        Schema schema = JsonSchemaParser.bqJson2BeamSchema(schemaStr);

        String username = options.getUsername();
        String password = options.getPassword();

        Read elasticReadTransform;
        ConnectionConfiguration connConfiguration;
        if (username.isEmpty()) {
            connConfiguration = ConnectionConfiguration.create(host, elasticIndex);
        } else {
            connConfiguration =
                    ConnectionConfiguration.create(host, elasticIndex)
                            .withUsername(username)
                            .withPassword(password);
        }

        if (query.isEmpty()) {
            elasticReadTransform =
                    ElasticsearchIO.read().withConnectionConfiguration(connConfiguration);
        } else {
            elasticReadTransform =
                    ElasticsearchIO.read()
                            .withConnectionConfiguration(connConfiguration)
                            .withQuery(query);
        }

        // Read data from Elastic as JSON strings
        PCollection<String> jsonStrings = p.apply("Read from Elastic", elasticReadTransform);

        // Parse JSON strings to Beam Row
        ParseResult parsingResult =
                jsonStrings.apply("Parse JSON", new Json2RowWithSanitization(schema));

        // This will have our schema
        PCollection<Row> correct = parsingResult.getResults().setRowSchema(schema);
        // And the failures will have ERROR_ROW_WITH_ERR_MSG_SCHEMA
        PCollection<Row> failed =
                parsingResult
                        .getFailedToParseLines()
                        .setRowSchema(JsonToRowWithErrFn.ERROR_ROW_SCHEMA);

        // Write to BigQuery
        correct.apply("Write correct to BQ", bqWriteForTable(correctTableRef));
        failed.apply("Write failures to BQ", bqWriteForTable(errorsTableRef));

        p.run(); // .waitUntilFinish();
    }

    private static Write<Row> bqWriteForTable(TableReference table) {
        return BigQueryIO.<Row>write()
                .to(table)
                .optimizedWrites()
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                .withMethod(Write.Method.FILE_LOADS)
                .useBeamSchema();
    }

    private static String readSchemaFile(String schemaLocation, String projectId)
            throws IOException {
        if (!schemaLocation.startsWith("gs:")) {
            System.out.println("The schema file must be located inside a GSC bucket.");
            System.exit(1);
        }

        File tempFile = File.createTempFile("schema", ".json");
        String pathStr = tempFile.getAbsolutePath();

        logger.info("Downloading schema to " + pathStr);

        Path path = Path.of(pathStr);
        StorageOptions options = StorageOptions.newBuilder().setProjectId(projectId).build();
        Storage storage = options.getService();
        Blob blob = storage.get(BlobId.fromGsUtilUri(schemaLocation));
        blob.downloadTo(path);
        String schemaStr = Files.readString(path);
        assert tempFile.delete();

        logger.info("Deleted " + pathStr);

        return schemaStr;
    }
}
