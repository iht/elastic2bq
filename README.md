# Elastic to BigQuery

This pipeline will take a ElasticSearch index and will create a table with the contents of that index in
BigQuery. 

The schema of the index can be inferred using a command line utility provided with the pipeline.

The input sources for this pipeline are the following:

* An Elastic search host and index
* A file in Google Cloud Storage with the schema of the index, in BigQuery JSON format.

The outputs of the pipeline are the following:

* A table in BigQuery with the contents of the index
* An errors table, for those JSON elements that could not be parsed, including information about the specific
  parsing error.

## Building the pipeline and the utility

You will need Java 17 to compile and run the pipeline and the utility.

For the build process, you need Maven.

If you have both installed, just run:

`mvn package`

This will create a package of name `elastic2bq-bundled-0.1-SNAPSHOT.jar` in the `target/` subdirectory.

## Schema inference utility

The inference utility depends on the BigQuery automatic schema detection when loading JSON data, so the
results will not be perfect, and you may have small inconsistencies. The utility is provided mainly to assist
you in the creation of a schema file.

Once you obtain a schema, it is advised to review the generated schema, and adjust any type that might not
have been inferred properly.

### Running the schema inference utility

The schema inference utility requires the JSON data to be located in Google Cloud Storage, in the form of
a file, with each JSON element in a single line.

Extract some data from Elastic, and if it is 

Once you have built the package, add the location to an environment variable in the shell

`export MYJAR=./target/elastic2bq-bundled-0.1-SNAPSHOT.jar`

and then run with the following options:

```shell
java -cp $JAR dev.herraiz.beam.cli.InferSchemaFromData \
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

## Running the pipeline locally

TODO

## Running the pipeline in Dataflow

TODO

## Google Cloud requirements

This pipeline requires you to have a Google Cloud project, where to store data in BigQuery and Google Cloud
Storage. The pipeline is inteded to be run in Dataflow, although with the corresponding additional runner
dependencies, it should run in any Beam runner. 

It can also be run with the DirectRunner, but you will still
need to have a BigQuery dataset and a Google Cloud Storage bucket for the pipeline to work.


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
