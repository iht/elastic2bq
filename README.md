# Elastic to BigQuery

This pipeline will take a ElasticSearch index and will create a table with the contents of that index in
BigQuery.

The schema of the index can be inferred using a command line utility provided with the pipeline.

The input sources for this pipeline are the following:

- An Elastic search host and index
- A file in Google Cloud Storage with the schema of the index, in BigQuery JSON format.

The outputs of the pipeline are the following:

- A table in BigQuery with the contents of the index
- An errors table, for those JSON elements that could not be parsed, including information about the specific
parsing error.

## Building the pipeline and the utility∏

You will need Java 17 to compile and run the pipeline and the utility.

For the build process, you need Gradle. Run the following script, and it should install all the
required Gradle dependencies if you don't have them already:

`./gradlew build`

This will create a package of name `elastic2bq-<COMMIT_HASH>-SNAPSHOT.jar` in the `build/` subdirectory.

## Schema inference utility

The inference utility depends on the BigQuery automatic schema detection when loading JSON data, so the
results will not be perfect, and you may have small inconsistencies. The utility is provided mainly to assist
you in the creation of a schema file.

Once you obtain a schema, it is advised to review the generated schema, and adjust any type that might not
have been inferred properly.

### Data format for the inference utility

The schema inference utility requires the JSON data to be located in Google Cloud Storage, in the form of
a file, with each JSON element in a single line.

To transform the data extracted from Elastic into a file with a single JSON element per line, you can use the
[`jq` utility](https://stedolan.github.io/jq/).

`cat mydata.json | jq -c > oneline_per_element.json`

Then upload the `oneline_per_element.json` file to Google Cloud Storage.

For an example of this format, have a look at the `data/commits.json` file in this repository.

### Running the schema inference utility

Once you have built the package, add the location to an environment variable in the shell

`export MYJAR=./target/elastic2bq-bundled-0.1-SNAPSHOT.jar`

and then run with the following options:

```shell
java -cp $MYJAR dev.herraiz.cli.InferSchemaFromData \
--dataset=<BIGQUERY DATASET> \
--project=<GCP PROJECT> \
--data=<GCS DATA LOCATION>
--output=<LOCAL OUTPUT FILE FOR SCHEMA>
```

You need to have a pre-existing BigQuery dataset, and the data already uploaded to Google Cloud Storage. Just
a small sample of data (50-100 records) should be enough to have a proper schema inferred.

The utility will create a temporary table in the dataset, and it will remove the table once the schema has
been inferred. The schema will be written to a local file.

The utility will refuse to overwrite the local output file for the schema, so the destination file must not
exist.

The output file must be local; you will need to upload it to Google Cloud Storage later.

## Running the pipeline locally

Build the package and export the location of the JAR:

`export MYJAR=./target/elastic2bq-bundled-0.1-SNAPSHOT.jar`

You can run the pipeline in local with these arguments:

```shell
java -cp $MYJAR dev.herraiz.beam.pipelines.Elastic2BQ \
--runner=DirectRunner \
--elasticHost="http://localhost:9200" \
--elasticIndex=<YOUR ELASTIC INDEX NAME> \
--project=<GCP PROJECT ID> \
--tempLocation=<GCS LOCATION FOR TEMPORARY FILES> \
--bigQueryDataset=<BIGQUERY DATASET ID> \
--bigQueryTable=<TABLE NAME FOR THE DATA> \
--bigQueryErrorsTable=<TABLE NAME FOR PARSING ERRORS> \
--schema=<GCS LOCATION OF SCHEMA FILE>
```

For reading from Elastic, you can also apply a query, using the option `--query`, to apply a query
to the index. The output of the query is what it will be written to BigQuery.

You can also optionally set a `--username` and `--password` to connect to Elastic.

For the BigQuery destination tables, you can also write each table to a different project and dataset, using
the options `--bigQueryProject`, `--bigQueryErrorsDataset` and/or `--bigQueryErrorsProject`. The datasets
must exist before running the pipeline, and the credentials must have permissions to create tables in those
datasets.

Here we assume that you are running with a local Elasticsearch server. See below for how to create one and
populate it with some data.

The schema must be located in Google Cloud Storage. If you have used the Schema Inference Utility, make sure
that you upload the generated file to GCS.

Once you have run the pipeline, you should see two new tables in the BigQuery dataset.

## Running the pipeline in Dataflow

The options are the same as in the case of the direct runner (except `--runner=DataflowRunner`),
but you may need to add additional options for networking, so the Dataflow workers can reach the
ElasticSearch server. For instance, the workers and the server may run in the same VPC, or you may need
to do VPC peering between the VPC where ElasticSearch is located and the workers' VPC. For more details, see:

- https://cloud.google.com/dataflow/docs/guides/specifying-networks

## Google Cloud requirements

Both the pipeline and the inference utility require to have access to Google Cloud credentials to use
BigQuery and Google Cloud Storage.

If you are using the Google Cloud SDK, make sure you configure it with your user and project id, and that
you run both:

`gcloud auth login`

and

`gcloud auth application-default login`

The user needs permission to run Dataflow jobs, to read and write from Google Cloud Storage, and to create
tables in the provided dataset in BigQuery.

The pipeline is intended to be run in Dataflow, although with the corresponding additional runner
dependencies, it should run in any Beam runner.

It can also be run with the DirectRunner, but you will still need to have a BigQuery dataset and a
Google Cloud Storage bucket for the pipeline to work.

## Getting some data to play with (for testing)

With minikube, you can easily install a Elasticsearch server in local, and use it to import some data, run
the pipeline locally, etc.

### Install Elastic in minikube

This is for testing purposes, to have a Elastic instance to run the pipeline.

Install minikube and helm (e.g. using Homebrew on Mac).

Then run minikube and follow these instructions to add Elastic to the minikube instance.

Create a namespace for Elastic:

`k create namespace elastic`

`helm repo add elastic https://helm.elastic.co`

In the `manifests` directory, run:

`helm install elasticsearch elastic/elasticsearch -f ./values.yaml -n elastic`

To make sure that the pod is running correctly, wait until it is ready. For a while, it will show something
like:

```
NAME                     READY   STATUS    RESTARTS   AGE
elasticsearch-master-0   0/1     Running   0          103s
elasticsearch-master-1   0/1     Running   0          103s
```

But after a couple of minutes, it should show like this:

```
NAME                     READY   STATUS    RESTARTS   AGE
elasticsearch-master-0   1/1     Running   0          2m
elasticsearch-master-1   1/1     Running   0          2m
```

Redirect the ports for Elastic to localhost, so you can use Elastic as a local service:

`k port-forward svc/elasticsearch-master 9200 -n elastic`

### Get some data to play with

Create index in Elastic:

```shell
curl --request PUT \
--url 'http://localhost:9200/git?pretty=' \
--header 'Connection: keep-alive'
```

Then import some sample data provided in this repository:

```shell
cat data/commits.json | while read l
do
curl --request POST \
	--url 'http://localhost:9200/git/_doc/?pretty=' \
	--header 'Content-Type: application/json' \
	--data "$l"
done
```

You can now run the pipeline locally.
