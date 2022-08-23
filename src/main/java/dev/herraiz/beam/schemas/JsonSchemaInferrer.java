package dev.herraiz.beam.schemas;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import java.util.List;
import java.util.UUID;

public class JsonSchemaInferrer {
  public static TableSchema inferSchemaFromSample(
      String jsonDataLocation, String project, String dataset) throws Exception {
    String tableUuid = UUID.randomUUID().toString();
    String tableName = "import-json-sample" + tableUuid;
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    TableId tableId = TableId.of(project, dataset, tableName);

    LoadJobConfiguration loadConfig =
        LoadJobConfiguration.newBuilder(tableId, jsonDataLocation)
            .setFormatOptions(FormatOptions.json())
            .setAutodetect(true)
            .build();
    Job job = bigquery.create(JobInfo.of(loadConfig));
    job = job.waitFor();
    if (job.isDone()) {
      Schema schema = bigquery.getTable(tableId).getDefinition().getSchema();
      bigquery.delete(tableId);  // Remove table after schema is read
      assert schema != null;
      return schema2TableSchema(schema);
    } else {
      throw new Exception(
          "BigQuery was unable to load into the table due to an error:"
              + job.getStatus().getError());
    }
  }

  private static TableSchema schema2TableSchema(Schema s) {
    List<TableFieldSchema> fields =
        s.getFields().stream().map(JsonSchemaInferrer::field2TableField).toList();

    return new TableSchema().setFields(fields);
  }

  private static TableFieldSchema field2TableField(Field f) {
    TableFieldSchema fieldSchemaPb = new TableFieldSchema();
    fieldSchemaPb.setName(f.getName());
    fieldSchemaPb.setType(f.getType().name());
    if (f.getMode() != null) {
      fieldSchemaPb.setMode(f.getMode().name());
    }

    if (f.getDescription() != null) {
      fieldSchemaPb.setDescription(f.getDescription());
    }

    if (f.getSubFields() != null) {
      List<TableFieldSchema> fieldsPb =
          f.getSubFields().stream().map(JsonSchemaInferrer::field2TableField).toList();
      fieldSchemaPb.setFields(fieldsPb);
    }

    return fieldSchemaPb;
  }
}
