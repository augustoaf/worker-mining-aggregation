# Quick Start Guide

## Prerequisites
- Java 11+ installed
- Maven 3.6+ installed
- RabbitMQ server running (default: localhost:5672)

## Quick Setup

1. **Start RabbitMQ** (if not already running):
   ```bash
   # Using Docker
   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
   ```

2. **Build the project**:
   ```bash
   mvn clean package
   ```

3. **Run the application**:
   ```bash
   # Option 1: Using the batch script (Windows)
   run.bat
   
   # Option 2: Using Maven
   mvn exec:java -Dexec.mainClass="com.equipment.aggregation.TemperatureAggregationJob"
   
   # Option 3: Using the JAR file
   java -jar target/worker-aggregation-1.0.0.jar
   ```

## Testing the Application

1. **Run the test producer** (sends sample events):
   ```bash
   mvn exec:java -Dexec.mainClass="com.equipment.aggregation.TestProducer"
   ```

2. **Run the test consumer** (reads aggregated results):
   ```bash
   mvn exec:java -Dexec.mainClass="com.equipment.aggregation.TestConsumer"
   ```

3. **Or use the test script**:
   ```bash
   test.bat
   ```

## What the Application Does

1. **Reads** equipment events from `raw_equipment_events` queue
2. **Processes** events in 10-second time windows
3. **Aggregates** temperature data by equipment ID
4. **Publishes** results to `agg_temperature` queue

## Input Format
```json
{
  "equipment_id": 1,
  "temperature": 53.54409802153736,
  "power": 1,
  "oil_level": 230.86633111771286,
  "timestamp": "2025-08-01T21:45:25.889291300"
}
```

## Output Format
```json
{
  "equipment_id": 1,
  "temperature_avg": 52.123456789,
  "timestamp": "2025-08-01T21:45:30.000",
  "count": 5
}
```

## Troubleshooting

- **Connection refused**: Make sure RabbitMQ is running on localhost:5672
- **Queue not found**: The application will create queues automatically
- **Build errors**: Make sure you have Java 11+ and Maven installed

## Monitoring

- **RabbitMQ Management UI**: http://localhost:15672 (guest/guest)
- **Application Logs**: Check console output for processing information 