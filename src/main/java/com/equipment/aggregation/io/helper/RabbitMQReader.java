package com.equipment.aggregation.io.helper;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;

import com.equipment.aggregation.io.RabbitMQSource;
import com.equipment.aggregation.model.EquipmentEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.*;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.NoSuchElementException;


/**
 * Reader implementation for RabbitMQ source.
 * this class is where the actual reading of messages from RabbitMQ takes place
 */
public class RabbitMQReader extends BoundedReader<EquipmentEvent> {
    
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQReader.class);

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
        this.config = source.getConfig();
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


