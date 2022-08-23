package dev.herraiz.beam.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface Elastic2BQOptions extends PipelineOptions {

  @Description("Hostname of the ElasticSearch server, including port")
  String getElasticHost();

  void setElasticHost(String s);

  @Description("ElasticSearch index to copy to BigQuery")
  String getElasticIndex();

  void setElasticIndex(String s);

  @Description("ELasticSearch type to copy to BigQuery")
  String getElasticType();

  void setElasticType(String s);

  @Description("BigQuery project")
  String getBigQueryProject();

  void setBigQueryProject(String s);

  @Required
  @Description("BigQuery dataset for the main table")
  String getBigQueryDataset();

  void setBigQueryDataset(String s);

  @Description("BigQuery destination table name")
  String getBigQueryTable();

  void setBigQueryTable(String s);

  @Description(
      "BigQuery project for the errors table. Optional, if not passed the same project will be used for errors")
  String getBigQueryErrorsProject();

  void setBigQueryErrorsProject(String s);

  @Description(
      "BigQuery dataset for the errors table. Optional, if not passed the same dataset will be used for errors")
  String getBigQueryErrorsDataset();

  void setBigQueryErrorsDataset(String s);

  @Description("BigQuery table name for the parsing errors")
  String getBigQueryErrorsTable();

  void setBigQueryErrorsTable(String s);

  @Description("Location of a JSON file with the schema of the data, in BQ JSON schema format")
  String getSchema();

  void setSchema(String s);
}
