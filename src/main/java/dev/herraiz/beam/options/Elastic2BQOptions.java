package dev.herraiz.beam.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

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

  @Description("BigQuery dataset -- it must exist previously")
  String getBigQueryDataset();

  void setBigQueryDataset(String s);

  @Description("BigQuery table")
  String getBigQueryTable();

  void setBigQueryTable(String s);

  @Description("Schema of the type in Elastic, it is the same schema used for BigQuery")
  String getSchema();

  void setSchema(String s);
}
