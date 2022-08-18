# Elastic to BigQuery

Dataflow pipeline to copy a elastic index and type into a table in BigQuery.

# Install Elastic and Kibana

This is for testing purposes, to have a Elastic instance to run the pipeline.

Install minikube and helm. Run minikube.

`k create namespace elastic`

`helm repo add elastic https://helm.elastic.co`

In the `manifests` directory, run:

`helm install elasticsearch elastic/elasticsearch -f ./values.yaml -n elastic`

`helm install kibana elastic/kibana -n elastic`

Redirect the ports for Elastic and Kibana to localhost:

`k port-forward deployment/kibana-kibana 5601 -n elastic`

`k port-forward svc/elasticsearch-master 9200 -n elastic`

# Get some data to play with

Install metricbeat

`helm install metricbeat elastic/metricbeat -n elastic`

There should be some data in `curl localhost:9200/_cat/indices`

# Get data from Git

Create index in Elastic

```shell
curl --request PUT \
--url 'http://localhost:9200/git?pretty=' \
--header 'Connection: keep-alive'
```

`cd data`

```shell
cat data/commits.json | while read l
do
  curl --request POST \
    --url 'http://localhost:9200/git/_doc/?pretty=' \
    --header 'Content-Type: application/json' \
    --data "$l"
done    
```