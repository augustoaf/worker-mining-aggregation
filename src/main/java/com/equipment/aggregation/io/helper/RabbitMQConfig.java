package com.equipment.aggregation.io.helper;

import java.io.Serializable;

/**
 * Configuration class for RabbitMQ connection.
 */
public class RabbitMQConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String host = "localhost";
    private int port = 5672;
    private String username = "guest";
    private String password = "guest";
    private String virtualHost = "/";
    private String queueName;
    private String dlqQueueName;

    public RabbitMQConfig(String queueName, String dlqQueueName) {
        this.queueName = queueName;
        this.dlqQueueName = dlqQueueName;
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

    public String getDlqQueueName() { return dlqQueueName; }
    public void setDlqQueueName(String dlqQueueName) { this.dlqQueueName = dlqQueueName; }
}
