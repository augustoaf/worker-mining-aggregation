package com.equipment.aggregation.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Objects;
 
/**
 * Data model representing an equipment event from RabbitMQ.
 */
public class EquipmentEvent implements Serializable {

    private static final long serialVersionUID = 1L;
 
    // Create a flexible DateTimeFormatter that can handle fractional seconds
    // from 0 to 9 digits.
    private static final DateTimeFormatter FLEXIBLE_FORMATTER = new DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
        .toFormatter();

    @JsonProperty("equipment_id")
    private Long equipmentId;
    
    @JsonProperty("temperature")
    private Double temperature;
    
    @JsonProperty("power")
    private Double power;
    
    @JsonProperty("oil_level")
    private Double oilLevel;
    
    @JsonProperty("timestamp")
    private String timestamp;
    
    // Default constructor for Jackson
    public EquipmentEvent() {}
    
    public EquipmentEvent(Long equipmentId, Double temperature, Double power, Double oilLevel, String timestamp) {
        this.equipmentId = equipmentId;
        this.temperature = temperature;
        this.power = power;
        this.oilLevel = oilLevel;
        this.timestamp = timestamp;
    }
    
    // Getters and setters
    public Long getEquipmentId() {
        return equipmentId;
    }
    
    public void setEquipmentId(Long equipmentId) {
        this.equipmentId = equipmentId;
    }
    
    public Double getTemperature() {
        return temperature;
    }
    
    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }
    
    public Double getPower() {
        return power;
    }
    
    public void setPower(Double power) {
        this.power = power;
    }
    
    public Double getOilLevel() {
        return oilLevel;
    }
    
    public void setOilLevel(Double oilLevel) {
        this.oilLevel = oilLevel;
    }
    
    public String getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    
    public Instant getTimestampAsInstant() {
        if (timestamp == null || timestamp.isEmpty()) {
            return null;
        }

        try {
            // Parse the timestamp string as LocalDateTime using the flexible formatter.
            // This handles varying fractional seconds.
            LocalDateTime localDateTime = LocalDateTime.parse(timestamp, FLEXIBLE_FORMATTER);
            
            // The timestamp does not include a timezone, so we assume it's UTC.
            // Convert the LocalDateTime to Instant with the UTC offset.
            return localDateTime.toInstant(ZoneOffset.UTC);
            
        } catch (DateTimeParseException e) {
            // If parsing fails for any reason, throw an informative exception.
            throw new RuntimeException("Unable to parse timestamp: " + timestamp, e);
        }
    }

    @Override
    public boolean equals(Object o) {
        // Step 1: Check for object identity. If it's the same object, they are equal.
        if (this == o) return true;
        // Step 2: Check for null and class type. If the object is null or not of the same class, they are not equal.
        if (o == null || getClass() != o.getClass()) return false;
        // Step 3: Cast the object to the correct type.
        EquipmentEvent that = (EquipmentEvent) o;
        // Step 4: Compare all relevant fields using Objects.equals() to handle nulls safely.
        // For Double, use Double.compare() to handle floating-point comparisons properly
        // (though Objects.equals works for object equality, it's good practice for primitives).
        return Objects.equals(equipmentId, that.equipmentId) &&
               Objects.equals(temperature, that.temperature) &&
               Objects.equals(power, that.power) &&
               Objects.equals(oilLevel, that.oilLevel) &&
               Objects.equals(timestamp, that.timestamp);
    }

    /**
     * The hashCode method is essential for using this class in hash-based collections
     * like HashMap or HashSet. The contract states that if two objects are equal
     * according to the equals() method, then their hash code must be the same.
     *
     * We use a combination of the hash codes of all relevant fields to generate a unique
     * hash code for each object. The Objects.hash() utility method is the recommended way
     * to do this in modern Java, as it provides a robust and concise way to combine hash codes.
     *
     * @return The hash code for this object.
     */
    @Override
    public int hashCode() {
        return Objects.hash(equipmentId, temperature, power, oilLevel, timestamp);
    }
}
