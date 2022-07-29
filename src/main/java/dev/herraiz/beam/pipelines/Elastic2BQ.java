package dev.herraiz.beam.pipelines;

import dev.herraiz.beam.options.Elastic2BQOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class Elastic2BQ {

  public static void main(String[] args) {
    Elastic2BQOptions options = PipelineOptionsFactory.fromArgs(args).as(Elastic2BQOptions.class);

    Pipeline p = Pipeline.create(options);

    String[] host = {options.getElasticHost()};
    PCollection<String> jsonStrings = p.apply(ElasticsearchIO.read().withConnectionConfiguration(
        ConnectionConfiguration.create(host, options.getElasticIndex(), options.getElasticType())
    ));
  }
}
