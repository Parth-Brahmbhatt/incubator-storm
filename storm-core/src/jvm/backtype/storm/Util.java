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

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.thrift.TException;

import java.io.Serializable;
import java.util.*;

/**
 * Created by pbrahmbhatt on 11/5/15.
 */
public class Util {

    private final NimbusClient client;

    public Util(Map config) {
        this.client = NimbusClient.getConfiguredClient(config);
    }

    public String getTopologyId(String topologyName) throws TException {
        ClusterSummary clusterInfo = this.client.getClient().getClusterInfo();
        List<TopologySummary> topologies = clusterInfo.get_topologies();
        if(topologies != null) {
            for(TopologySummary summary: topologies) {
                if(topologyName.equals(summary.get_name())) {
                    return summary.get_id();
                }
            }
        }
        return null;
    }

    public void getSome(String topologyId) throws TException {
        StormTopology topology = this.client.getClient().getTopology(topologyId);
        for (Map.Entry<String, Bolt> entry : topology.get_bolts().entrySet()) {
            String boltName = entry.getKey();
            Serializable instance = Utils.javaDeserialize(entry.getValue().get_bolt_object().get_serialized_java(), Serializable.class);
            System.out.println(instance);
        }
    }

    public String getOwner(String topologyId) throws TException {
        return this.client.getClient().getTopologyInfo(topologyId).get_owner();
    }

    public Set<String> getSpoutNames(String topologyId) throws TException {
        StormTopology topology = this.client.getClient().getTopology(topologyId);
        return topology.get_spouts().keySet();
    }

    public Set<String> getBoltNames(String topologyId) throws TException {
        StormTopology topology = this.client.getClient().getTopology(topologyId);
        return topology.get_bolts().keySet();
    }

    public Set<String> getTerminalUserBoltNames(String topologyId) throws TException {
        StormTopology topology = this.client.getClient().getTopology(topologyId);
        Set<String> terminalBolts = new HashSet<>();
        Set<String> inputs = new HashSet<>();
        for (Map.Entry<String, Bolt> entry : topology.get_bolts().entrySet()) {
            String name = entry.getKey();
            Set<GlobalStreamId> inputsForBolt = entry.getValue().get_common().get_inputs().keySet();
            if (!isSystemComponent(name)) {
                for (GlobalStreamId streamId : inputsForBolt) {
                    inputs.add(streamId.get_componentId());
                }
            }
        }

        for (String boltName : topology.get_bolts().keySet()) {
            if (!isSystemComponent(boltName) && !inputs.contains(boltName)) {
                terminalBolts.add(boltName);
            }
        }

        return terminalBolts;
    }

    public Map<String, Set<String>> getAdjacencyMap(String topologyId, boolean removeSystemComponent) throws TException {
        Map<String, Set<String>> adjacencyMap = new HashMap<>();
        StormTopology topology = this.client.getClient().getTopology(topologyId);

        for(Map.Entry<String, Bolt> entry : topology.get_bolts().entrySet()) {
            String boltName = entry.getKey();
            Map<GlobalStreamId, Grouping> inputs = entry.getValue().get_common().get_inputs();
            for(Map.Entry<GlobalStreamId, Grouping> input : inputs.entrySet()) {
                String inputComponentId = input.getKey().get_componentId();
                Set<String> components = adjacencyMap.containsKey(inputComponentId) ? adjacencyMap.get(inputComponentId) : new HashSet<String>();
                components.add(boltName);
                components = removeSystemComponent ? removeSystemComponents(components) : components;
                if((removeSystemComponent && !isSystemComponent(inputComponentId)) || !removeSystemComponent) {
                    adjacencyMap.put(inputComponentId, components);
                }
            }
        }
        return adjacencyMap;
    }



    public boolean isSystemComponent(String componentName) {
        return componentName.startsWith("__");
    }

    public Set<String> removeSystemComponents(Set<String> components) {
        Set<String> userComponents = new HashSet<>();
        for (String component : components) {
            if (!isSystemComponent(component))
                userComponents.add(component);
        }
        return userComponents;
    }

    public static void main(String args[]) throws TException {
        Map map = Utils.readDefaultConfig();
        Util util = new Util(map);
        String topologyId = util.getTopologyId("wordcount");
        System.out.println("all bolts = " + util.getBoltNames(topologyId));
        System.out.println("user bolts = " + util.removeSystemComponents(util.getBoltNames(topologyId)));
        System.out.println("spouts = " + util.getSpoutNames(topologyId));
        System.out.println("terminal user bolts = " + util.getTerminalUserBoltNames(topologyId));
        System.out.println("adjacency map without system component = " + util.getAdjacencyMap(topologyId, true));
        System.out.println("adjacency map with system component = " + util.getAdjacencyMap(topologyId, false));
        util.getSome(topologyId);
    }
}
