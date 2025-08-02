package com.equipment.aggregation;

import com.equipment.aggregation.model.TemperatureAggregation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple test consumer to read aggregated temperature results from RabbitMQ.
 * This is useful for verifying the aggregation pipeline output.
 */
public class TestConsumer {
    
    private static final Logger LOG = LoggerFactory.getLogger(TestConsumer.class);
    private static final String QUEUE_NAME = "agg_temperature";
    
    public static void main(String[] args) {
        try {
            // Setup RabbitMQ connection
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            factory.setPort(5672);
            factory.setUsername("guest");
            factory.setPassword("guest");
            factory.setVirtualHost("/");
            
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            
            LOG.info("Starting to consume aggregated results from queue: {}", QUEUE_NAME);
            
            // Create consumer
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                         AMQP.BasicProperties properties, byte[] body) {
                    try {
                        String message = new String(body, "UTF-8");
                        TemperatureAggregation aggregation = objectMapper.readValue(message, TemperatureAggregation.class);
                        
                        LOG.info("Received aggregation: Equipment ID={}, Temperature Avg={}, Timestamp={}, Count={}",
                                aggregation.getEquipmentId(),
                                aggregation.getTemperatureAvg(),
                                aggregation.getTimestamp(),
                                aggregation.getCount());
                        
                        // Acknowledge the message
                        channel.basicAck(envelope.getDeliveryTag(), false);
                        
                    } catch (Exception e) {
                        LOG.error("Error processing message", e);
                        try {
                            channel.basicNack(envelope.getDeliveryTag(), false, true);
                        } catch (Exception ex) {
                            LOG.error("Error nacking message", ex);
                        }
                    }
                }
            };
            
            // Start consuming
            channel.basicConsume(QUEUE_NAME, false, consumer);
            
            LOG.info("Consumer started. Press Ctrl+C to stop.");
            
            // Keep the consumer running
            while (true) {
                Thread.sleep(1000);
            }
            
        } catch (Exception e) {
            LOG.error("Error in test consumer", e);
        }
    }
} 