package backtype.storm.serialization;

import org.apache.thrift.TBase;

import java.util.Map;

/**
 * Created by pbrahmbhatt on 1/23/15.
 */
public class ThriftSerilizationDelegateBridge implements SerializationDelegate {
    private SerializationDelegate thriftSerializationDelegate;
    private SerializationDelegate defaultSerializationDelegate;

    @Override
    public void prepare(Map stormConf) {
        this.thriftSerializationDelegate = new ThriftSerializationDelegate();
        this.thriftSerializationDelegate.prepare(stormConf);

        this.defaultSerializationDelegate = new DefaultSerializationDelegate();
        this.defaultSerializationDelegate.prepare(stormConf);
    }

    @Override
    public byte[] serialize(Object object) {
        if(object instanceof TBase) {
            return thriftSerializationDelegate.serialize(object);
        } else {
            return defaultSerializationDelegate.serialize(object);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) {
        if(TBase.class.isAssignableFrom(clazz)) {
            return thriftSerializationDelegate.deserialize(bytes, clazz);
        } else {
            return defaultSerializationDelegate.deserialize(bytes, clazz);
        }
    }
}
