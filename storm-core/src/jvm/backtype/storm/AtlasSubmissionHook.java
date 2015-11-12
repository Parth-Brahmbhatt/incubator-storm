/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.SpoutSpec;
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

        for (Map.Entry<String, SpoutSpec> entry : topology.get_spouts().entrySet()) {
            String name = entry.getKey();
            Serializable instance = Utils.javaDeserialize(entry.getValue().get_spout_object().get_serialized_java(), Serializable.class);
            Map<String, String> flatConfigMap = ReflectionUtil.getFieldValues(instance, true);
            nameToInstance.put(name, instance);
            nameToFlatConfigMap.put(name, flatConfigMap);
        }

        System.out.println(nameToInstance);
        System.out.println(nameToFlatConfigMap);
    }
}
