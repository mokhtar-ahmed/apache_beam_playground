package jobs;

import com.google.api.services.bigquery.model.TableRow;
import models.SensorEvent;
import models.SensorEventBuilder;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;


public class SensorXmlEventToBQJob {


    public interface MyPipelineOptions extends DataflowPipelineOptions {

        @Description("PubSub topic")
        @Default.String("sensor_events")
        String getTopic();
        void setTopic(String topic);

        @Description("BQ table name")
        @Default.String("dummy-table")
        String getBqTable();
        void setBqTable(String bqTable);

    }


    public static void main(String[] args) {


        PipelineOptionsFactory.register(MyPipelineOptions.class);
        MyPipelineOptions ops = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(MyPipelineOptions.class);

        String topic = "projects/" + ops.getProject() + "/topics/"+ ops.getTopic();
        String sensorEventsTable = ops.getProject() + ":" + ops.getBqTable();

        Pipeline pipe = Pipeline.create(ops);

        pipe
                .apply("Read xml events",  PubsubIO.readStrings().fromTopic(topic) )
                .apply("Parse xml events", MapElements.into(new TypeDescriptor<SensorEvent>(){})
                        .via( (String l) -> SensorEventBuilder.buildEvent(l) ))
                .apply("Convert to BQ row", MapElements.into(new TypeDescriptor<TableRow>() {})
                        .via( e -> SensorEventBuilder.toBQRow(e)))
                .apply("Write to BQ", BigQueryIO.writeTableRows().to(sensorEventsTable)
                        .withSchema(SensorEventBuilder.getBQTableSchema())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));


        pipe.run();
    }
}
