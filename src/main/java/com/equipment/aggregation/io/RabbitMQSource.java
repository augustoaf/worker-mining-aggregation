package com.equipment.aggregation.io;

import com.equipment.aggregation.io.helper.RabbitMQConfig;
import com.equipment.aggregation.io.helper.RabbitMQReader;
import com.equipment.aggregation.model.EquipmentEvent;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Bounded source for reading equipment events from RabbitMQ.
 * 
 * Note: Having a message queue considered as a Bounded source, the objective is to drain the queue, then complete the pieline.
 *
 */
public class RabbitMQSource extends BoundedSource<EquipmentEvent> {
    
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSource.class);
    
    private final RabbitMQConfig config;
    
    public RabbitMQSource(RabbitMQConfig config) {
        this.config = config;
    }

    public RabbitMQConfig getConfig() {
        return this.config;
    }
    
    //The split method is crucial for Beam's parallel processing model. 
    //Its purpose is to divide the data source into smaller, independent BoundedSource objects. 
    //Each of these sub-sources can then be processed by a different worker.
    //In this specific implementation, the split method simply returns a list containing only 
    //itself (sources.add(this)). This is a common pattern for bounded sources that cannot be effectively split
    @Override
    public List<BoundedSource<EquipmentEvent>> split(long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
        List<BoundedSource<EquipmentEvent>> sources = new ArrayList<>();
        sources.add(this); // provide the entire source data ('this') because there is no way to achieve parallelism with Apache Beam BoundedSource implementation with a single queue.
        return sources;
    }
    
    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return 1024; // Default estimate - you can provide a real estimation, but it is pointless for a BoundedSource implementation for a queue (which is a streaming)
    }
    
    //The Beam runner calls this method on each worker that has been assigned a BoundedSource to process.
    @Override
    public BoundedReader<EquipmentEvent> createReader(PipelineOptions options) throws IOException {
        return new RabbitMQReader(this);
    }
    
    @Override
    public Coder<EquipmentEvent> getOutputCoder() {
        return SerializableCoder.of(EquipmentEvent.class);
    }    
}
