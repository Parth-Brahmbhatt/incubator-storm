package backtype.storm.serialization;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;


public class JsonSerializationDelegate implements SerializationDelegate {
    private ObjectMapper mapper;

    @Override
    public void prepare(Map stormConf) {
        mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(Object object) {
        try {
            StringWriter writer = new StringWriter();
            mapper.writeValue(writer, object);
            return writer.toString().getBytes();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize " + object, e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try {
            return mapper.readValue(bytes, clazz);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize " + new String(bytes), e);
        }
    }
}
