package backtype.storm.serialization;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.TBase;

import java.util.Map;

public class ThriftSerializationDelegate implements SerializationDelegate {

    @Override
    public void prepare(Map stormConf) {
    }

    @Override
    public byte[] serialize(Object object) {
        try {
            return  new TSerializer().serialize((TBase) object);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try {
            TBase instance = (TBase) clazz.newInstance();
            new TDeserializer().deserialize(instance, bytes);
            return (T)instance;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
