package com.equipment.aggregation.io;

import com.equipment.aggregation.model.EquipmentEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Bounded source for reading equipment events from RabbitMQ.
 */
public class RabbitMQSource extends BoundedSource<EquipmentEvent> {
    
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSource.class);
    
    private final RabbitMQConfig config;
    
    public RabbitMQSource(RabbitMQConfig config) {
        this.config = config;
    }
    
    //The split method is crucial for Beam's parallel processing model. 
    //Its purpose is to divide the data source into smaller, independent BoundedSource objects. 
    //Each of these sub-sources can then be processed by a different worker.
    //In this specific implementation, the split method simply returns a list containing only 
    //itself (sources.add(this)). This is a common pattern for bounded sources that cannot be effectively split
    @Override
    public List<BoundedSource<EquipmentEvent>> split(long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
        List<BoundedSource<EquipmentEvent>> sources = new ArrayList<>();
        sources.add(this);
        return sources;
    }
    
    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return 1024; // Default estimate
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
    
    /**
     * Reader implementation for RabbitMQ source.
     * this class is where the actual reading of messages from RabbitMQ takes place
     */
    public static class RabbitMQReader extends BoundedReader<EquipmentEvent> {
        
        private final RabbitMQSource source;
        private final RabbitMQConfig config;

        private transient ObjectMapper objectMapper;
        
        private Connection connection;
        private Channel channel;
        private GetResponse response;
        private EquipmentEvent currentElement;
        private boolean started = false;
        
        public RabbitMQReader(RabbitMQSource source) {
            this.source = source;
            this.config = source.config;
        }

        // method to initialize the ObjectMapper on the worker
        private void initializeObjectMapper() {
            if (this.objectMapper == null) {
                this.objectMapper = new ObjectMapper();
                this.objectMapper.registerModule(new JavaTimeModule());
            }
        }
        
        //it is triggered once per BundedReader when worker start processing
        @Override
        public boolean start() throws IOException {
            
            initializeObjectMapper();
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost(config.getHost());
                factory.setPort(config.getPort());
                factory.setUsername(config.getUsername());
                factory.setPassword(config.getPassword());
                factory.setVirtualHost(config.getVirtualHost());
                
                connection = factory.newConnection();
                channel = connection.createChannel();
                channel.queueDeclare(config.getQueueName(), true, false, false, null);
                
                started = true;
                LOG.info("Started reading from RabbitMQ queue: {}", config.getQueueName());
                return advance();
                
            } catch (Exception e) {
                LOG.error("Error starting RabbitMQ reader", e);
                throw new IOException("Failed to start RabbitMQ reader", e);
            }
        }
        
        //This is the core reading loop method. 
        //It is called repeatedly by the Beam runner to read the next element from the source.
        @Override
        public boolean advance() throws IOException {
            try {
                if (!started) {
                    return start();
                }
                
                response = channel.basicGet(config.getQueueName(), false);
                if (response != null) {
                    String message = new String(response.getBody());
                    currentElement = objectMapper.readValue(message, EquipmentEvent.class);
                    
                    // Acknowledge the message
                    channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                    
                    LOG.debug("Received event: {}", currentElement);
                    return true;
                }
                return false;
                
            } catch (Exception e) {
                LOG.error("Error advancing RabbitMQ reader", e);
                return false;
            }
        }
         
        @Override
        public EquipmentEvent getCurrent() throws NoSuchElementException {
            if (currentElement == null) {
                throw new NoSuchElementException();
            }
            return currentElement;
        }
        
        @Override
        public BoundedSource<EquipmentEvent> getCurrentSource() {
            return source;
        }
        
        //The close method is called once per BoundedReader when it has finished processing all its elements
        @Override
        public void close() throws IOException {
            try {
                if (channel != null) {
                    channel.close();
                }
                if (connection != null) {
                    connection.close();
                }
                LOG.info("Closed RabbitMQ reader");
            } catch (Exception e) {
                LOG.error("Error closing RabbitMQ reader", e);
            }
        }
    }
    
    /**
     * Configuration class for RabbitMQ connection.
     */
    public static class RabbitMQConfig implements Serializable {
        
        private static final long serialVersionUID = 1L;
        
        private String host = "localhost";
        private int port = 5672;
        private String username = "guest";
        private String password = "guest";
        private String virtualHost = "/";
        private String queueName;
        
        public RabbitMQConfig(String queueName) {
            this.queueName = queueName;
        }
        
        // Getters and setters
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        
        public String getVirtualHost() { return virtualHost; }
        public void setVirtualHost(String virtualHost) { this.virtualHost = virtualHost; }
        
        public String getQueueName() { return queueName; }
        public void setQueueName(String queueName) { this.queueName = queueName; }
    }
}
