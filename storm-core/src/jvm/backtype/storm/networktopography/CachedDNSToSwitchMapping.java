/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.networktopography;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A cached implementation of DNSToSwitchMapping that takes an
 * raw DNSToSwitchMapping and stores the resolved network location in 
 * a cache. The following calls to a resolved network location
 * will get its location from the cache. 
 *
 */
public class CachedDNSToSwitchMapping extends AbstractDNSToSwitchMapping {
  private Map<String, String> cache = new ConcurrentHashMap<String, String>();

  /**
   * The uncached mapping
   */
  protected final DNSToSwitchMapping rawMapping;

  /**
   * cache a raw DNS mapping
   * @param rawMapping the raw mapping to cache
   */
  public CachedDNSToSwitchMapping(DNSToSwitchMapping rawMapping) {
    this.rawMapping = rawMapping;
  }

  /**
   * @param names a list of hostnames to probe for being cached
   * @return the hosts from 'names' that have not been cached previously
   */
  private List<String> getUncachedHosts(List<String> names) {
    // find out all names without cached resolved location
    List<String> unCachedHosts = new ArrayList<String>(names.size());
    for (String name : names) {
      if (cache.get(name) == null) {
        unCachedHosts.add(name);
      }
    }
    return unCachedHosts;
  }

  /**
   * Caches the resolved host:rack mappings. The two list
   * parameters must be of equal size.
   *
   * @param uncachedHosts a list of hosts that were uncached
   * @param resolvedHosts a list of resolved host entries where the element
   * at index(i) is the resolved value for the entry in uncachedHosts[i]
   */
  private void cacheResolvedHosts(List<String> uncachedHosts,
      List<String> resolvedHosts) {
    // Cache the result
    if (resolvedHosts != null) {
      for (int i=0; i<uncachedHosts.size(); i++) {
        cache.put(uncachedHosts.get(i), resolvedHosts.get(i));
      }
    }
  }

  /**
   * @param names a list of hostnames to look up (can be be empty)
   * @return the cached resolution of the list of hostnames/addresses.
   *  or null if any of the names are not currently in the cache
   */
  private List<String> getCachedHosts(List<String> names) {
    List<String> result = new ArrayList<String>(names.size());
    // Construct the result
    for (String name : names) {
      String networkLocation = cache.get(name);
      if (networkLocation != null) {
        result.add(networkLocation);
      } else {
        return null;
      }
    }
    return result;
  }

  @Override
  public Map<String,String> resolve(List<String> names) {
    // normalize all input names to be in the form of IP addresses
    names = normalizeHostNames(names);

    Map<String,String> result = new HashMap();
    Map<String,String> result = new HashMap<>();
    if (names.isEmpty()) {
      return result;
    }

    // returns a subset of the hostnames provided whose value in the cache is null.
    // getUncachedHosts assumes every "names" provided will be present in the cache otherwise will cause NullPointerException.
    List<String> uncachedHosts = getUncachedHosts(names);
    Map<String, String> resolvedUncachedHosts = rawMapping.resolve(uncachedHosts);
    // update the cache
    cache.putAll(resolvedUncachedHosts);
//    = new ArrayList<String>(rawMapping.resolve(uncachedHosts).keySet());

    for (int i = 0; i < resolvedUncachedHosts.size(); i++){
      resolvedHosts.add(resolvedUncachedHosts.get(uncachedHosts.get(i)));
    }

    List<String> resolvedHostsFromCache = getCachedHosts(names);
      result.put(names.get(i), resolvedHostsFromCache.get(i));
    }
    return result;
//    return getCachedHosts(names);

    // query the cache for all the hostnames provided
    for (String host : names){
      result.put(host, cache.get(host));
    }

    // return map of resolved hosts to network paths.
    return result;
  }

  /**
   * Given a string representation of a host, return its ip address
   * in textual presentation.
   *
   * @param name a string representation of a host:
   *             either a textual representation its IP address or its host name
   * @return its IP address in the string format
   */
  private static String normalizeHostName(String name) {
    try {
      return InetAddress.getByName(name).getHostAddress();
    } catch (UnknownHostException e) {
      return name;
    }
  }

  /**
   * Given a collection of string representation of hosts, return a list of
   * corresponding IP addresses in the textual representation.
   *
   * @param names a collection of string representations of hosts
   * @return a list of corresponding IP addresses in the string format
   * @see #normalizeHostName(String)
   */
  private static List<String> normalizeHostNames(Collection<String> names) {
    List<String> hostNames = new ArrayList<String>(names.size());
    for (String name : names) {
      hostNames.add(normalizeHostName(name));
    }
    return hostNames;
  }

  /**
   * Get the (host x switch) map.
   * @return a copy of the cached map of hosts to rack
   */
  @Override
  public Map<String, String> getSwitchMap() {
    Map<String, String > switchMap = new HashMap<String, String>(cache);
    return switchMap;
  }


  @Override
  public String toString() {
    return "cached switch mapping relaying to " + rawMapping;
  }
  
  @Override
  public void reloadCachedMappings() {
    cache.clear();
  }

  @Override
  public void reloadCachedMappings(List<String> names) {
    for (String name : names) {
      cache.remove(name);
    }
  }
}
