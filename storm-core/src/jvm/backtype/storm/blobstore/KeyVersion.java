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

package backtype.storm.blobstore;

import backtype.storm.utils.Utils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeSet;
import java.util.Map;
import java.util.List;

/**
 * Class hands over the version of the key to be stored within the zookeeper
 */
public class KeyVersion {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
  private final String BLOBSTORE_SUBTREE="/blobstore";
  private final String BLOBSTORE_KEY_COUNTER_SUBTREE="/blobstorekeycounter";
  private String key;

  public KeyVersion(String key) {
    this.key = key;
  }

  public int getKey(Map conf) {
    TreeSet<Integer> versions = new TreeSet<Integer>();
    CuratorFramework zkClient = Utils.createZKClient(conf);
    try {
      // Key has not been created yet and it is the first time it is being created
      if(zkClient.checkExists().forPath(BLOBSTORE_SUBTREE + "/" + key) == null) {
        LOG.info("checkexists");
        zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(BLOBSTORE_KEY_COUNTER_SUBTREE + "/" + key + "/" + 1);
        return 1;
      }

      // When all nimbodes go down and one or few of them come up
      List<String> stateInfoList = zkClient.getChildren().forPath(BLOBSTORE_SUBTREE + "/" + key);
      LOG.info("stateInfoList {} stateInfoList {}", stateInfoList.size(), stateInfoList);
      if(stateInfoList.isEmpty()) {
        LOG.info("empty");
        return getKeyCounterValue(zkClient, key);
      }

      LOG.debug("stateInfoSize {}", stateInfoList.size());
      // In all other cases check for the latest version on the nimbus and assign the version
      // check if all are have same version, if not assign the highest version
      for (String stateInfo:stateInfoList) {
        LOG.debug("enter loop");
        versions.add(Integer.parseInt(Utils.normalizeVersionInfo(stateInfo)[1]));
      }
      if (stateInfoList.size() > 1 && versions.size() == 1) {
        LOG.debug("enter  size");
        if (versions.first() < getKeyCounterValue(zkClient, key)) {
          LOG.info("counter value");
          int currentCounter = getKeyCounterValue(zkClient, key);
          zkClient.delete().deletingChildrenIfNeeded().forPath(BLOBSTORE_KEY_COUNTER_SUBTREE + "/" + key);
          zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                  .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(BLOBSTORE_KEY_COUNTER_SUBTREE + "/" + key + "/" + (currentCounter + 1));
          return currentCounter + 1;
        } else {
          LOG.info("version value");
          return versions.first() + 1;
        }
      }
    } catch(Exception e) {
      LOG.error("Exception {}", e);
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
    LOG.debug("versions");
    return versions.last();
  }

  public int getKeyCounterValue(CuratorFramework zkClient, String key) throws Exception {
    return Integer.parseInt(zkClient.getChildren()
            .forPath(BLOBSTORE_KEY_COUNTER_SUBTREE + "/" + key).get(0));
  }
}
