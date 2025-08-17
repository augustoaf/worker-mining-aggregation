package com.equipment.aggregation;

import com.equipment.aggregation.io.RabbitMQSource;
import com.equipment.aggregation.io.helper.RabbitMQConfig;
import com.equipment.aggregation.io.RabbitMQSink;
import com.equipment.aggregation.model.EquipmentEvent;
import com.equipment.aggregation.model.TemperatureAggregation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import java.io.Serializable;

/**
 * Apache Beam job for aggregating equipment temperature data from RabbitMQ.
 * 
 * This job:
 * 1. Reads equipment events from the 'raw_equipment_events' topic
 * 2. Groups events by equipment_id and 10-second time windows
 * 3. Calculates the average temperature and count for each equipment in each window
 * 4. Publishes aggregated results to the 'agg_temperature' topic
 */
public class TemperatureAggregationJob {

    private static final Logger LOG = LoggerFactory.getLogger(TemperatureAggregationJob.class);

    // Queue names
    private static final String INPUT_QUEUE = "raw_equipment_events";
    private static final String OUTPUT_QUEUE = "agg_temperature";

    // Window duration
    private static final Duration WINDOW_DURATION = Duration.standardSeconds(10);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // Read from RabbitMQ
        RabbitMQSource rabbitMQSource = new RabbitMQSource(new RabbitMQConfig(INPUT_QUEUE));
        PCollection<EquipmentEvent> events = pipeline
                .apply("Read from RabbitMQ", org.apache.beam.sdk.io.Read.from(rabbitMQSource))
                .setCoder(SerializableCoder.of(EquipmentEvent.class));

        // Apply windowing and aggregation
        PCollection<TemperatureAggregation> aggregations = events
                .apply("Add Timestamps", ParDo.of(new AddTimestampsFn()))//add a timestamp to each EquipmentEvent object in the PCollection in order to enable aggregation in a time-based window - Data in streaming pipeline does not have an inherited timestamp
                .apply("Key by Equipment ID", WithKeys.of(EquipmentEvent::getEquipmentId))//prepare the data to 'key-value based' in order to enable aggregation by key > converts the PCollection<EquipmentEvent> into PCollection<KV<Long, EquipmentEvent>> 
                .setCoder(KvCoder.of(VarLongCoder.of(), SerializableCoder.of(EquipmentEvent.class)))//identify the Coder for serialize/deserialize for the new PCollection key-value based.
                .apply("Window into 10s", Window.into(FixedWindows.of(WINDOW_DURATION)))//group all the data according the window specified > like multiple PCollections considering the windows for the whole dataset 
                .apply("Aggregate Temperature", Combine.perKey(new TemperatureAggregator()))// group the data per key inside each window and run the aggregation > The output of this step will be a PCollection<KV<Long, AggregationResult>>
                .apply("Convert to Aggregation", ParDo.of(new ConvertToAggregationFn()));// transform the output of the Combine.perKey step into the final desired format (TemperatureAggregation object)

        // Write to RabbitMQ
        aggregations.apply("Write to RabbitMQ", RabbitMQSink.writeTo(OUTPUT_QUEUE));

        LOG.info("Starting temperature aggregation pipeline...");
        pipeline.run().waitUntilFinish();
        LOG.info("Pipeline completed successfully");
    }

    /**
     * DoFn to add timestamps to events (EquipmentEvent object) based on their timestamp field.
     */
    public static class AddTimestampsFn extends DoFn<EquipmentEvent, EquipmentEvent> {

        @ProcessElement
        public void processElement(ProcessContext context) {
            EquipmentEvent event = context.element();
            Instant timestamp = new Instant(event.getTimestampAsInstant().toEpochMilli());
            context.outputWithTimestamp(event, timestamp);
        }
    }

    /**
     * A custom class to hold the aggregated results (average and count).
     */
    public static class AggregationResult implements Serializable {
        private static final long serialVersionUID = 1L;
        public double averageTemperature;
        public long count;

        public AggregationResult(double averageTemperature, long count) {
            this.averageTemperature = averageTemperature;
            this.count = count;
        }
    }

    /**
     * Combiner for aggregating temperature values.
     * The output type is AggregationResult
     */
    public static class TemperatureAggregator extends Combine.CombineFn<EquipmentEvent, TemperatureAggregator.Accum, AggregationResult> {

        public static class Accum implements Serializable {
            private static final long serialVersionUID = 1L;
            double sum = 0.0;
            long count = 0;
        }

        @Override
        public Accum createAccumulator() {
            return new Accum();
        }

        @Override
        public Accum addInput(Accum accumulator, EquipmentEvent input) {
            if (input.getTemperature() != null) {
                accumulator.sum += input.getTemperature();
                accumulator.count++;
            }
            return accumulator;
        }

        @Override
        public Accum mergeAccumulators(Iterable<Accum> accumulators) {
            Accum merged = createAccumulator();
            for (Accum accumulator : accumulators) {
                merged.sum += accumulator.sum;
                merged.count += accumulator.count;
            }
            return merged;
        }

        @Override
        // The extractOutput returns the final desired AggregationResult object
        public AggregationResult extractOutput(Accum accumulator) {
            double avg = accumulator.count > 0 ? accumulator.sum / accumulator.count : 0.0;
            return new AggregationResult(avg, accumulator.count);
        }
    }

    /**
     * DoFn to convert aggregated results to TemperatureAggregation objects.
     * This DoFn receives a KV with our custom AggregationResult.
     */
    public static class ConvertToAggregationFn extends DoFn<KV<Long, AggregationResult>, TemperatureAggregation> {

        private static final DateTimeFormatter ISO_LOCAL_DATE_TIME = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

        @ProcessElement
        public void processElement(ProcessContext context, BoundedWindow window) {
            KV<Long, AggregationResult> element = context.element();
            Long equipmentId = element.getKey();
            AggregationResult result = element.getValue();

            // Get the window end timestamp and format it correctly
            IntervalWindow intervalWindow = (IntervalWindow) window;
            String timestamp = intervalWindow.end().toString(ISO_LOCAL_DATE_TIME);

            TemperatureAggregation aggregation = new TemperatureAggregation(
                    equipmentId,
                    result.averageTemperature,
                    timestamp,
                    result.count
            );

            context.output(aggregation);
        }
    }
}
