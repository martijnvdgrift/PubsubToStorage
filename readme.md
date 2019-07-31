# PubSub to Storage

This is a project which contains some example Apache Beam pipelines.
Use this as a guideline for a new pipeline which takes data from Pub/Sub and stores it in Google Cloud Storage

## Run minimum word count on DataFlow
- mvn package
-  java -cp target/PubSubToStorage-bundled-0.1.jar io.binx.dataflow.ingest.examples.MinimalWordCount --runner=DataflowRunner