package backtype.storm;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AtlasSubmissionHook implements ISubmitterHook {


    @Override
    public void notify(String topologyName, Map stormConf, StormTopology topology) throws IllegalAccessException {
        Map<String, Serializable> nameToInstance = new HashMap<>();
        Map<String, Map<String, String>> nameToFlatConfigMap = new HashMap<>();
        for (Map.Entry<String, Bolt> entry : topology.get_bolts().entrySet()) {
            String name = entry.getKey();
            Serializable instance = Utils.javaDeserialize(entry.getValue().get_bolt_object().get_serialized_java(), Serializable.class);
            Map<String, String> flatConfigMap = ReflectionUtil.getFieldValues(instance, true);
            nameToInstance.put(name, instance);
            nameToFlatConfigMap.put(name, flatConfigMap);
        }

        System.out.println(nameToInstance);
        System.out.println(nameToFlatConfigMap);
    }
}
