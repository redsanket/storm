package backtype.storm.testing;

import backtype.storm.networktopography.CachedDNSToSwitchMapping;
import backtype.storm.networktopography.DNSToSwitchMapping;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements the {@link DNSToSwitchMapping} interface
 *    It alternates bewteen RACK1 and RACK2 for the hosts.
 */
public final class AlternateRackDNSToSwitchMapping extends CachedDNSToSwitchMapping {

    private Map<String, String> mappingCache = new ConcurrentHashMap<String, String>();

    public AlternateRackDNSToSwitchMapping() {
        super(null);
    }

    @Override
    public Map<String, String> resolve(List<String> names) {

        Map <String, String> m = new HashMap<String, String>();
        if (names.isEmpty()) {
            //name list is empty, return an empty map
            return m;
        }

        Boolean odd = true;
        for (String name : names) {
            if (odd) {
                m.put(name, "RACK1");
                mappingCache.put(name, "RACK1");
                odd = false;
            } else {
                m.put(name, "RACK2");
                mappingCache.put(name, "RACK2");
                odd = true;
            }
        }
        return m;
    }

    @Override
    public String toString() {
        return "defaultRackDNSToSwitchMapping (" + mappingCache.size() + " mappings cached)";
    }
}