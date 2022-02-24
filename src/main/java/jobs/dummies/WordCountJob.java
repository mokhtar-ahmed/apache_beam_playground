package jobs.dummies;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;


public class WordCountJob {

    public interface TestPipelineOptions extends PipelineOptions {

        @Description("Input for the pipeline")
        @Default.String("src/main/resources/my-bucket/input/input.txt")
        String getInput();
        void setInput(String input);

        @Description("Output for the pipeline")
        @Default.String("src/main/resources/my-bucket/output/")
        String getOutput();
        void setOutput(String output);


    }


    public static class ComputeLength extends DoFn<String, String> {
        @ProcessElement
        public  void processElement(@Element String line, OutputReceiver<String> out) {
            String length = String.valueOf(line.length());
            System.out.println(length);
            out.output(length);
        }

    }

    public static void main(String[] args) {

        PipelineOptionsFactory.register(TestPipelineOptions.class);
        TestPipelineOptions testOption = PipelineOptionsFactory.fromArgs(args)
                                                               .withValidation()
                                                               .as(TestPipelineOptions.class);

        Pipeline p = Pipeline.create(testOption);

        PCollection<String> lines = p.apply(
                "Read Text Files From Bucket",
                TextIO.read().from(testOption.getInput())
        );


        PCollection<String> output = lines
                .apply(MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                        .via(l -> Arrays.asList(l.split(" "))))
                .apply(Flatten.<String>iterables())
                .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                        .via(w -> KV.of(w.toLowerCase(), 1)))
                .apply(Combine.perKey( (a, b) -> a + b))
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via( el -> el.getKey() + ", " + el.getValue()));

        output.apply(
                "write lengths to file",
                TextIO.write().to(testOption.getOutput()).withNumShards(1).withSuffix(".txt")
        );

        p.run();
    }
}
