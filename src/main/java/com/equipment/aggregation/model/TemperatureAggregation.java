package com.equipment.aggregation.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

/**
 * Data model representing aggregated temperature data for an equipment.
 */
public class TemperatureAggregation implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @JsonProperty("equipment_id")
    private Long equipmentId;
    
    @JsonProperty("temperature_avg")
    private Double temperatureAvg;
    
    @JsonProperty("timestamp")
    private String timestamp;
    
    @JsonProperty("count")
    private Long count;
    
    // Default constructor for Jackson
    public TemperatureAggregation() {}
    
    public TemperatureAggregation(Long equipmentId, Double temperatureAvg, String timestamp, Long count) {
        this.equipmentId = equipmentId;
        this.temperatureAvg = temperatureAvg;
        this.timestamp = timestamp;
        this.count = count;
    }
    
    // Getters and setters
    public Long getEquipmentId() {
        return equipmentId;
    }
    
    public void setEquipmentId(Long equipmentId) {
        this.equipmentId = equipmentId;
    }
    
    public Double getTemperatureAvg() {
        return temperatureAvg;
    }
    
    public void setTemperatureAvg(Double temperatureAvg) {
        this.temperatureAvg = temperatureAvg;
    }
    
    public String getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    
    public Long getCount() {
        return count;
    }
    
    public void setCount(Long count) {
        this.count = count;
    }
    
    @Override
    public String toString() {
        return "TemperatureAggregation{" +
                "equipmentId=" + equipmentId +
                ", temperatureAvg=" + temperatureAvg +
                ", timestamp='" + timestamp + '\'' +
                ", count=" + count +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        // Step 1: Check for object identity. If it's the same object, they are equal.
        if (this == o) return true;
        // Step 2: Check for null and class type. If the object is null or not of the same class, they are not equal.
        if (o == null || getClass() != o.getClass()) return false;
        // Step 3: Cast the object to the correct type.
        TemperatureAggregation that = (TemperatureAggregation) o;
        // Step 4: Compare all relevant fields using Objects.equals() to handle nulls safely.
        return Objects.equals(equipmentId, that.equipmentId) &&
               Objects.equals(temperatureAvg, that.temperatureAvg) &&
               Objects.equals(timestamp, that.timestamp) &&
               Objects.equals(count, that.count);
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
        return Objects.hash(equipmentId, temperatureAvg, timestamp, count);
    }
}
