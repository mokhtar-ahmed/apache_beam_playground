package org.example.jobs;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.values.PCollection;

public abstract class AbstractPipeline {

    public abstract PCollection<String> extract() ;
    public abstract PCollection<TableRow> transform(PCollection<String> dataset);
    public abstract WriteResult load(PCollection<TableRow> dataset);

    public void execute(Pipeline pipeline){
        PCollection<String> inputDS = extract();
        PCollection<TableRow> transformedDS = transform(inputDS);
        load(transformedDS);
        pipeline.run();
    }

}
