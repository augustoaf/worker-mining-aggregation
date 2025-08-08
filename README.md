# Worker Aggregation - Apache Beam Temperature Aggregation

This Apache Beam application (with custom BoundedSource) reads equipment events from a message queue, aggregates temperature data over 10-second time windows per equipment, and publishes the results to another message queue.

Important: This implementation considers a custom Apache Beam BoundedSource for RabbitMQ Message Broker just to drain the Queue. Usually for streamings like a message queue and to achieve parallelism, the implementation needs to consider UnboundedSource.

## Features

- **RabbitMQ Integration**: Reads from `raw_equipment_events` queue and writes to `agg_temperature` queue
- **Time-based Aggregation**: Groups events by equipment ID and 10-second time windows
- **Temperature Averaging**: Calculates average temperature for each equipment in each window
- **Real-time Processing**: Uses Apache Beam for stream processing

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- RabbitMQ server running (default: localhost:5672)

## Project Structure

```
worker-aggregation/
├── src/main/java/com/equipment/aggregation/
│   ├── model/
│   │   ├── EquipmentEvent.java          # Data model for input events
│   │   └── TemperatureAggregation.java  # Data model for aggregated results
│   ├── io/
│   │   ├── RabbitMQSource.java          # Custom source for reading from RabbitMQ
│   │   ├── RabbitMQSink.java            # Custom sink for writing to RabbitMQ
│   │   └── EquipmentEventCoder.java     # Custom coder for serialization
│   └── TemperatureAggregationJob.java   # Main Apache Beam pipeline
├── pom.xml                              # Maven configuration
└── README.md                            # This file
```

## Data Format

### Input Event Format (raw_equipment_events queue)
```json
{
  "equipment_id": 1,
  "temperature": 53.54409802153736,
  "power": 1,
  "oil_level": 230.86633111771286,
  "timestamp": "2025-08-01T21:45:25.889291300"
}
```

### Output Aggregation Format (agg_temperature queue)
```json
{
  "equipment_id": 1,
  "temperature_avg": 52.123456789,
  "timestamp": "2025-08-01T21:45:30.000",
  "count": 5
}
```

## Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd worker-aggregation
   ```

2. **Build the project**:
   ```bash
   mvn clean package
   ```

3. **Start RabbitMQ** (if not already running):
   ```bash
   # Using Docker
   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
   
   # Or install and start RabbitMQ service
   ```

4. **Create the required queues**:
   ```bash
   # Using RabbitMQ Management UI (http://localhost:15672)
   # Or using rabbitmqctl
   rabbitmqctl add_queue raw_equipment_events
   rabbitmqctl add_queue agg_temperature
   ```

## Running the Application

### Option 1: Run with Maven
```bash
mvn exec:java -Dexec.mainClass="com.equipment.aggregation.TemperatureAggregationJob"
```

### Option 2: Run the JAR file
```bash
java -jar target/worker-aggregation-1.0.0.jar
```

### Option 3: Run with custom RabbitMQ configuration
```bash
mvn exec:java -Dexec.mainClass="com.equipment.aggregation.TemperatureAggregationJob" \
    -Dexec.args="--rabbitmqHost=your-rabbitmq-host --rabbitmqPort=5672"
```

## Configuration

The application uses default RabbitMQ connection settings:
- **Host**: localhost
- **Port**: 5672
- **Username**: guest
- **Password**: guest
- **Virtual Host**: /

To customize these settings, modify the `RabbitMQConfig` class in `RabbitMQSource.java` or extend the application to accept command-line arguments.

## How It Works

1. **Source**: The application reads JSON messages from the `raw_equipment_events` RabbitMQ queue
2. **Parsing**: Each message is parsed into an `EquipmentEvent` object
3. **Windowing**: Events are grouped into 10-second fixed time windows
4. **Grouping**: Events are grouped by `equipment_id` within each window
5. **Aggregation**: Temperature values are averaged for each equipment in each window
6. **Output**: Aggregated results are published to the `agg_temperature` RabbitMQ queue

## Testing

### Send Test Messages

You can send test messages to the input queue using the RabbitMQ Management UI or a simple producer:

```java
// Example test message
String testMessage = "{\"equipment_id\":1,\"temperature\":53.54409802153736,\"power\":1,\"oil_level\":230.86633111771286,\"timestamp\":\"2025-08-01T21:45:25.889291300\"}";
```

### Monitor Results

Check the `agg_temperature` queue for aggregated results. Each result will contain:
- `equipment_id`: The equipment identifier
- `temperature_avg`: Average temperature for the 10-second window
- `timestamp`: When the aggregation was calculated
- `count`: Number of events processed (if available)

## Troubleshooting

### Common Issues

1. **Connection refused**: Ensure RabbitMQ is running and accessible
2. **Queue not found**: Create the required queues before running the application
3. **Serialization errors**: Check that input JSON matches the expected format
4. **Memory issues**: Adjust JVM heap size for large datasets

### Logs

The application uses SLF4J logging. Set the log level in your JVM arguments:
```bash
-Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG
```

## Performance Considerations

- The application uses Apache Beam's Direct Runner by default
- For production use, consider using a distributed runner like Dataflow or Aoache Flink
- Adjust window size and batch processing based on your throughput requirements