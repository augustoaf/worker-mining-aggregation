package com.equipment.aggregation.io;

import com.equipment.aggregation.model.EquipmentEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.util.VarInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Custom coder for EquipmentEvent serialization.
 */
public class EquipmentEventCoder extends CustomCoder<EquipmentEvent> {
    
    private static final EquipmentEventCoder INSTANCE = new EquipmentEventCoder();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    static {
        objectMapper.registerModule(new JavaTimeModule());
    }
    
    public static EquipmentEventCoder of() {
        return INSTANCE;
    }
    
    @Override
    public void encode(EquipmentEvent value, OutputStream outStream) throws CoderException, IOException {
        if (value == null) {
            VarInt.encode(-1, outStream);
        } else {
            String json = objectMapper.writeValueAsString(value);
            byte[] bytes = json.getBytes("UTF-8");
            VarInt.encode(bytes.length, outStream);
            outStream.write(bytes);
        }
    }
    
    @Override
    public EquipmentEvent decode(InputStream inStream) throws CoderException, IOException {
        int length = VarInt.decodeInt(inStream);
        if (length == -1) {
            return null;
        }
        
        byte[] bytes = new byte[length];
        inStream.read(bytes);
        String json = new String(bytes, "UTF-8");
        return objectMapper.readValue(json, EquipmentEvent.class);
    }
    
    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        throw new NonDeterministicException(this, "JSON serialization is not deterministic");
    }
} 