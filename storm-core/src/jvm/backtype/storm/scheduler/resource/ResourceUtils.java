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

package backtype.storm.scheduler.resource;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ResourceUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);

    public static Map<String, Map<String, Double>> getBoltsResources(StormTopology topology, Map topologyConf) {
        Map<String, Map<String, Double>> boltResources = new HashMap<String, Map<String, Double>>();
        if (topology.get_bolts() != null) {
            for (Map.Entry<String, Bolt> bolt : topology.get_bolts().entrySet()) {
                Map<String, Double> topology_resources = parseResources(bolt.getValue().get_common().get_json_conf());
                checkIntialization(topology_resources, bolt.getValue().toString(), topologyConf);
                boltResources.put(bolt.getKey(), topology_resources);
            }
        }
        return boltResources;
    }

    public static Map<String, Map<String, Double>> getSpoutsResources(StormTopology topology, Map topologyConf) {
        Map<String, Map<String, Double>> spoutResources = new HashMap<String, Map<String, Double>>();
        if (topology.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spout : topology.get_spouts().entrySet()) {
                Map<String, Double> topology_resources = parseResources(spout.getValue().get_common().get_json_conf());
                checkIntialization(topology_resources, spout.getValue().toString(), topologyConf);
                spoutResources.put(spout.getKey(), topology_resources);
            }
        }
        return spoutResources;
    }

    public static void checkIntialization(Map<String, Double> topology_resources, String Com, Map topologyConf) {
        checkInitMem(topology_resources, Com, topologyConf);
        checkInitCPU(topology_resources, Com, topologyConf);
    }

    public static void checkInitMem(Map<String, Double> topology_resources, String Com, Map topologyConf) {
        if (!topology_resources.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)) {
            topology_resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
                    backtype.storm.utils.Utils.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null));
            debugMessage("ONHEAP", Com, topologyConf);
        }
        if (!topology_resources.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)) {
            topology_resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB,
                    backtype.storm.utils.Utils.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null));
            debugMessage("OFFHEAP", Com, topologyConf);
        }
    }

    public static void checkInitCPU(Map<String, Double> topology_resources, String Com, Map topologyConf) {
        if (!topology_resources.containsKey(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)) {
            topology_resources.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT,
                    backtype.storm.utils.Utils.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT), null));
            debugMessage("CPU", Com, topologyConf);
        }
    }

    public static Map<String, Double> parseResources(String input) {
        Map<String, Double> topology_resources = new HashMap<String, Double>();
        JSONParser parser = new JSONParser();
        LOG.debug("Input to parseResources {}", input);
        try {
            if (input != null) {
                Object obj = parser.parse(input);
                JSONObject jsonObject = (JSONObject) obj;
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)) {
                    Double topoMemOnHeap = backtype.storm.utils.Utils
                            .getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null);
                    topology_resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, topoMemOnHeap);
                }
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)) {
                    Double topoMemOffHeap = backtype.storm.utils.Utils
                            .getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null);
                    topology_resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, topoMemOffHeap);
                }
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)) {
                    Double topoCPU = backtype.storm.utils.Utils.getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT), null);
                    topology_resources.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, topoCPU);
                }
                LOG.debug("Topology Resources {}", topology_resources);
            }
        } catch (ParseException e) {
            LOG.error("Failed to parse component resources is:" + e.toString(), e);
            return null;
        }
        return topology_resources;
    }

    private static void debugMessage(String memoryType, String Com, Map topologyConf) {
        if (memoryType.equals("ONHEAP")) {
            LOG.debug(
                    "Unable to extract resource requirement for Component {} \n Resource : Memory Type : On Heap set to default {}",
                    Com, topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB));
        } else if (memoryType.equals("OFFHEAP")) {
            LOG.debug(
                    "Unable to extract resource requirement for Component {} \n Resource : Memory Type : Off Heap set to default {}",
                    Com, topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB));
        } else {
            LOG.debug(
                    "Unable to extract resource requirement for Component {} \n Resource : CPU Pcore Percent set to default {}",
                    Com, topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));
        }
    }
}
