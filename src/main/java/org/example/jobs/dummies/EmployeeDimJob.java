package org.example.jobs.dummies;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;


public class EmployeeDimJob {


    public interface TestPipelineOptions extends PipelineOptions {

        @Description("Input for the pipeline")
        @Default.String("src/main/resources/my-bucket/input/employees.csv")
        String getEmployeeInputPath();
        void setEmployeeInputPath(String employeeInputPath);

        @Description("Input for the pipeline")
        @Default.String("src/main/resources/my-bucket/input/departments.csv")
        String getDepInputPath();
        void setDepInputPath(String DepInputPath);

        @Description("Output for the pipeline")
        @Default.String("src/main/resources/my-bucket/output/")
        String getOutput();
        void setOutput(String output);

    }

    private static final Schema EMP_SCHEMA =
            Schema.builder()
                    .addStringField("id")
                    .addStringField("name")
                    .addStringField("salary")
                    .addStringField("department_id")
                    .build();

    private static final Schema DEPT_SCHEMA =
            Schema.builder()
                    .addStringField("id")
                    .addStringField("name")
                    .build();


    private static final Schema EMP_DEPT_SCHEMA =
            Schema.builder()
                    .addStringField("id")
                    .addStringField("name")
                    .addStringField("salary")
                    .addStringField("department_id")
                    .addStringField("dept_name")
                    .build();


    public static void main(String[] args) {

        PipelineOptionsFactory.register(TestPipelineOptions.class);
        TestPipelineOptions testOption = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(TestPipelineOptions.class);

        Pipeline p = Pipeline.create(testOption);

        PCollection<Row> empPC = p
                .apply(TextIO.read().from(testOption.getEmployeeInputPath()))
                .apply(Filter.by( l -> l.startsWith("id") == false))
                .apply(MapElements.into(TypeDescriptors.rows())
                        .via( e -> Row.withSchema(EMP_SCHEMA).addValues(e.split(",")).build()))
                .setRowSchema(EMP_SCHEMA);

        PCollection<Row> depPC = p
                .apply(TextIO.read().from(testOption.getDepInputPath()))
                .apply(Filter.by( l -> l.startsWith("id") == false))
                .apply(MapElements.into(TypeDescriptors.rows())
                        .via( e -> Row.withSchema(DEPT_SCHEMA).addValues(e.split(",")).build()))
                .setRowSchema(DEPT_SCHEMA);

        //empPC.apply(MapElements.into(TypeDescriptors.voids()).via( (l) -> {System.out.println(l); return null;}));
        //depPC.apply(MapElements.into(TypeDescriptors.voids()).via( (l) -> {System.out.println(l); return null;}));


        PCollection<Row> output  = empPC.apply(Join.<Row,Row>innerJoin(depPC).on(
                Join.FieldsEqual.left("department_id").right("id")
        ));

        output.apply(MapElements.into(TypeDescriptors.voids()).via( (l) -> {System.out.println(l); return null;}));

        p.run();
    }
}
