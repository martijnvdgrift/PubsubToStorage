package io.binx.dataflow.ingest;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;

public class PubSubToStoragePipeline {

    public static void main(String[] args) {
        String targetBucket = ""; // todo: set your bucket name here in format: gs://my-bucket-name/

        PipelineOptions options = PipelineOptionsFactory.create();
        DataflowPipelineOptions dataflowPipelineOptions = options.as(DataflowPipelineOptions.class);
        dataflowPipelineOptions.setRunner(DataflowRunner.class);
        dataflowPipelineOptions.setGcpTempLocation(targetBucket + "temp");
        dataflowPipelineOptions.setWorkerMachineType("n1-standard-4");
        dataflowPipelineOptions.setRegion("europe-west4");
        dataflowPipelineOptions.setProject(""); //todo: your project-id here
        Pipeline p = Pipeline.create(options);


//        p.apply() //todo: create a PubsubIO source which reads PubsubMessages
//        .apply() //todo: Create a PTransform, to extract the message
//        .apply() //todo: Write a file to google cloud storage with TextIO for every Pubsub message

        p.run().waitUntilFinish();
    }
}
