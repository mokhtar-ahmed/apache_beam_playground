package org.example.runners;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowProfilingOptions;
import org.apache.beam.runners.dataflow.options.DataflowProfilingOptions.DataflowProfilingAgentConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.example.jobs.AbstractPipeline;
import org.example.jobs.SensorXmlEventToBQJob;
import org.example.models.SensorEvent;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.MAX_VALUE;


public class SensorRunner{




    public static void main(String[] args) {
        SensorXmlEventToBQJob job = new SensorXmlEventToBQJob();
        job.init(args);
        job.execute(job.getPipe());
    }
}
