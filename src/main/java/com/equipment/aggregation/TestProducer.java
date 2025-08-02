package com.equipment.aggregation;

import com.equipment.aggregation.model.EquipmentEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Random;

/**
 * Simple test producer to send sample equipment events to RabbitMQ.
 * This is useful for testing the aggregation pipeline.
 */
public class TestProducer {
    
    private static final Logger LOG = LoggerFactory.getLogger(TestProducer.class);
    private static final String QUEUE_NAME = "raw_equipment_events";
    
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
            
            Random random = new Random();
            
            LOG.info("Starting to send test messages to queue: {}", QUEUE_NAME);
            
            // Send messages for 60 seconds
            long endTime = System.currentTimeMillis() + 60000; // 60 seconds
            
            while (System.currentTimeMillis() < endTime) {
                // Generate random equipment event
                EquipmentEvent event = generateRandomEvent(random);
                
                String message = objectMapper.writeValueAsString(event);
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
                
                LOG.info("Sent event: {}", event);
                
                // Wait 1-3 seconds between messages
                Thread.sleep(1000 + random.nextInt(2000));
            }
            
            channel.close();
            connection.close();
            
            LOG.info("Test producer finished");
            
        } catch (Exception e) {
            LOG.error("Error in test producer", e);
        }
    }
    
    private static EquipmentEvent generateRandomEvent(Random random) {
        long equipmentId = 1 + random.nextInt(5); // Equipment IDs 1-5
        double temperature = 20.0 + random.nextDouble() * 60.0; // Temperature 20-80°C
        double power = random.nextDouble() * 100.0; // Power 0-100
        double oilLevel = 100.0 + random.nextDouble() * 200.0; // Oil level 100-300
        String timestamp = Instant.now().toString();
        
        return new EquipmentEvent(equipmentId, temperature, power, oilLevel, timestamp);
    }
} 