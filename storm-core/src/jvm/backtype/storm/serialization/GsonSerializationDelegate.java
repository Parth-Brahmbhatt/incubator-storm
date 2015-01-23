package backtype.storm.serialization;

import com.google.gson.Gson;

import java.util.Map;

/**
 * Created by pbrahmbhatt on 1/22/15.
 */
public class GsonSerializationDelegate implements SerializationDelegate {

    @Override
    public void prepare(Map stormConf) {

    }

    @Override
    public byte[] serialize(Object object) {
        Gson gson = new Gson();
        return gson.toJson(object).getBytes();
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        Gson gson = new Gson();
        return gson.fromJson(new String(bytes), clazz);
    }
}
