## Running the Kafka Consumer Application

### Pre-requisites
1. Java and SpringBoot
2. Before running this app, please initialize Zookeeper, Kafka and ElasticSearch service locally.

### Application
- This application subscribes to the Kafka queue
- REST Api calls that are queued in Kafka are consumed by this app and pushed on to Elasticsearch
- Any REST Api request that is generated from the main application is queue
