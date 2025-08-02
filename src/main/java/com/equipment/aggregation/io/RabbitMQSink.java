package com.equipment.aggregation.io;

import com.equipment.aggregation.model.TemperatureAggregation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RabbitMQ sink for writing aggregated temperature data.
 */
public class RabbitMQSink {
     
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSink.class);
    
    /**
     * Creates a PTransform that writes TemperatureAggregation objects to RabbitMQ.
     */
    public static WriteToRabbitMQ writeTo(String queueName) {
        return new WriteToRabbitMQ(queueName);
    }
    
    /**
     * PTransform for writing to RabbitMQ.
     */
    public static class WriteToRabbitMQ extends PTransform<PCollection<TemperatureAggregation>, PCollection<Void>> {
        
        private final String queueName;
        
        public WriteToRabbitMQ(String queueName) {
            this.queueName = queueName;
        }
        
        @Override
        public PCollection<Void> expand(PCollection<TemperatureAggregation> input) {
            return input.apply(ParDo.of(new RabbitMQWriter(queueName)));
        }
    }
    
    /**
     * DoFn for writing individual messages to RabbitMQ.
     */
    public static class RabbitMQWriter extends DoFn<TemperatureAggregation, Void> {
        
        private final String queueName;
        private transient ObjectMapper objectMapper;
        
        private transient Connection connection;
        private transient Channel channel;
        
        public RabbitMQWriter(String queueName) {
            this.queueName = queueName;
        }
        
        @Setup
        public void setup() throws Exception {

            this.objectMapper = new ObjectMapper();
            this.objectMapper.registerModule(new JavaTimeModule());

            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setPort(5672);
            factory.setUsername("guest");
            factory.setPassword("guest");
            factory.setVirtualHost("/");
            
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(queueName, true, false, false, null);
            
            LOG.info("Connected to RabbitMQ and declared queue: {}", queueName);
        }
        
        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {
            TemperatureAggregation aggregation = context.element();
            
            try {
                String message = objectMapper.writeValueAsString(aggregation);
                channel.basicPublish("", queueName, null, message.getBytes("UTF-8"));
                
                LOG.debug("Published aggregation to queue {}: {}", queueName, aggregation);
                
            } catch (Exception e) {
                LOG.error("Error publishing message to RabbitMQ", e);
                throw e;
            }
        }
        
        @Teardown
        public void teardown() throws Exception {
            try {
                if (channel != null) {
                    channel.close();
                }
                if (connection != null) {
                    connection.close();
                }
                LOG.info("Closed RabbitMQ connection");
            } catch (Exception e) {
                LOG.error("Error closing RabbitMQ connection", e);
            }
        }
    }
} 