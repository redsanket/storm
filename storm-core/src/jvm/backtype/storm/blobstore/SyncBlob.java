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

import backtype.storm.generated.ReadableBlobMeta;
import backtype.storm.nimbus.NimbusInfo;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Thread wakes up periodically and updates the nimbus with blobs based on the state stored inside the zookeeper
 */
public class SyncBlob implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(SyncBlob.class);
  private CuratorFramework zkClient;
  private Map conf;
  BlobStore blobStore;

  public SyncBlob(BlobStore blobStore, Map conf, CuratorFramework zkClient) {
    this.blobStore = blobStore;
    this.zkClient = zkClient;
    this.conf = conf;
  }

  public void run() {
    try {
      Thread.sleep(300);
      while (true) {
        List<String> keyListBlobStore = getKeyListFromBlobStore();
        updateKeyListForBlobStore(keyListBlobStore);
        List<String> keyListZookeeper = getKeyListFromZookeeper();
        List<String> keyListToDownload = getKeyListToDownload(keyListBlobStore, keyListZookeeper);
        LOG.info("Key List B->Z->DL {}-> {}-> {}", keyListBlobStore, keyListZookeeper, keyListToDownload);
        for (String key : keyListToDownload) {
          List<NimbusInfo> nimbusInfoList = Utils.getNimbodesWithLatestVersionOfBlob(zkClient, key);
          Utils.downloadBlob(conf, blobStore, key, nimbusInfoList);
        }
        Thread.sleep(300);
      }
    } catch(InterruptedException exp){
        LOG.error("InterruptedException {}", exp); // Do we want to log exception or throw it up the stack
    } catch(TTransportException exp){
        throw new RuntimeException(exp);
    } catch(Exception exp){
        throw new RuntimeException(exp);
    }
  }

  // Update current key list inside the blobstore if the version changes
  public void updateKeyListForBlobStore(List<String> keyListBlobStore) {
    try {
      for (String key : keyListBlobStore) {
        List<String> stateInfo = zkClient.getChildren().forPath("/blobstore/" + key);
        ReadableBlobMeta rbm = blobStore.getBlobMeta(key, Utils.getNimbusSubject());
        if (rbm.get_version() < Utils.getLatestVersion(stateInfo)) {
          List<NimbusInfo> nimbusList = Utils.getNimbodesWithLatestVersionOfBlob(zkClient, key);
          Utils.downloadBlob(conf, blobStore, key, nimbusList);
        }
      }
    } catch (Exception exp) {
        throw new RuntimeException(exp);
    }
  }

  // Get the list of keys from blobstore
  public List<String> getKeyListFromBlobStore() throws Exception {
    Iterator<String> keys = blobStore.listKeys(null);
    List<String> keyList = new ArrayList<String>();
    while (keys != null && keys.hasNext()) {
      keyList.add(keys.next());
    }
    return keyList;
  }

  // Get all the keys present on various nimbodes
  public List<String> getKeyListFromZookeeper() throws Exception {
    return zkClient.getChildren().forPath("/blobstore");
  }

  // Make a key list to download
  public List<String> getKeyListToDownload(List<String> blobStoreKeyList, List<String> zookeeperKeyList) {
    LOG.info("zookeeperList {}, blobKeyStoreList {}", zookeeperKeyList, blobStoreKeyList);
    zookeeperKeyList.removeAll(blobStoreKeyList);
    LOG.info("zookeeperList {}, blobKeyStoreList {}", zookeeperKeyList, blobStoreKeyList);
    return zookeeperKeyList;
  }

    // Create blobs after getting the objects
}
