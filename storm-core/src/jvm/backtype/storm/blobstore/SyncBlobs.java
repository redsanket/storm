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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * Is called periodically and updates the nimbus with blobs based on the state stored inside the zookeeper
 */
public class SyncBlobs {
  private static final Logger LOG = LoggerFactory.getLogger(SyncBlobs.class);
  private CuratorFramework zkClient;
  private Map conf;
  private BlobStore blobStore;
  private List<String> blobStoreKeyList = new ArrayList<String>();
  private List<String> zookeeperKeyList = new ArrayList<String>();
  private NimbusInfo nimbusInfo;


  public SyncBlobs(BlobStore blobStore, Map conf) {
    this.blobStore = blobStore;
    this.conf = conf;
  }

  public void setNimbusInfo(NimbusInfo nimbusInfo) {
    this.nimbusInfo = nimbusInfo;
  }

  public void setZookeeperKeyList(List<String> zookeeperKeyList) {
    this.zookeeperKeyList = zookeeperKeyList;
  }

  public void setBlobStoreKeyList(List<String> blobStoreKeyList) {
    this.blobStoreKeyList = blobStoreKeyList;
  }

  public NimbusInfo getNimbusInfo() {
    return nimbusInfo;
  }

  public List<String> getBlobStoreKeyList() {
    List<String> keyList = new ArrayList<String>();
    keyList.addAll(blobStoreKeyList);
    return keyList;
  }

  public List<String> getZookeeperKeyList() {
    List<String> keyList = new ArrayList<String>();
    keyList.addAll(zookeeperKeyList);
    return keyList;
  }

  public synchronized void syncBlobs() {
    try {
    LOG.debug("Sync blobs - blobstore {} keys {} zookeeperkeys {}", blobStore, getBlobStoreKeyList(), getZookeeperKeyList());
    zkClient = Utils.createZKClient(conf);
    deleteKeyListFromBlobStoreNotOnZookeeper(getBlobStoreKeyList(), getZookeeperKeyList());
    updateKeyListForBlobStore(getBlobStoreKeyList());
    List<String> keyListToDownload = getKeyListToDownload(getBlobStoreKeyList(), getZookeeperKeyList());
    LOG.debug("Key List Blobstore-> Zookeeper-> DownloadList {}-> {}-> {}", getBlobStoreKeyList(), getZookeeperKeyList(), keyListToDownload);

    for (String key : keyListToDownload) {
      List<NimbusInfo> nimbusInfoList = Utils.getNimbodesWithLatestVersionOfBlob(zkClient, key);
      if(Utils.downloadBlob(conf, blobStore, key, nimbusInfoList)) {
        Utils.createStateInZookeeper(conf, key, nimbusInfo);
      }
    }
    if (zkClient !=null) {
      zkClient.close();
    }
    } catch(InterruptedException exp) {
        LOG.error("InterruptedException {}", exp);
    } catch(TTransportException exp) {
        throw new RuntimeException(exp);
    } catch(Exception exp) {
      // Should we log or throw exception
        throw new RuntimeException(exp);
    }
  }

  public void deleteKeyListFromBlobStoreNotOnZookeeper(List<String> keyListBlobStore, List<String> keyListZookeeper) throws Exception {
    if (keyListBlobStore.removeAll(keyListZookeeper)
            || (keyListZookeeper.isEmpty() && !keyListBlobStore.isEmpty())) {
      LOG.debug("Key List to delete in blobstore {}", keyListBlobStore);
      for (String key : keyListBlobStore) {
       blobStore.deleteBlob(key, Utils.getNimbusSubject());
      }
    }
  }

  // Update current key list inside the blobstore if the version changes
  public void updateKeyListForBlobStore(List<String> keyListBlobStore) {
    try {
      List<String> stateInfo = new ArrayList<String>();
      for (String key : keyListBlobStore) {
        if(zkClient.checkExists().forPath("/blobstore/" + key) == null) {
          return;
        }
        stateInfo = zkClient.getChildren().forPath("/blobstore/" + key);
        ReadableBlobMeta rbm = blobStore.getBlobMeta(key, Utils.getNimbusSubject());
        LOG.debug("StateInfo {} readable blob meta-data {}", stateInfo.toString(), rbm.get_version());
        if (rbm.get_version() < Utils.getLatestVersion(stateInfo)) {
          List<NimbusInfo> nimbusInfoList = Utils.getNimbodesWithLatestVersionOfBlob(zkClient, key);
          if(Utils.downloadBlob(conf, blobStore, key, nimbusInfoList)) {
            LOG.debug("Updating state inside zookeeper");
            Utils.createStateInZookeeper(conf, key, nimbusInfo);
          }
        }
      }
    } catch (Exception exp) {
        throw new RuntimeException(exp);
    }
  }

  // Make a key list to download
  public List<String> getKeyListToDownload(List<String> blobStoreKeyList, List<String> zookeeperKeyList) {
    zookeeperKeyList.removeAll(blobStoreKeyList);
    LOG.debug("Key list to download zookeeperList {}", zookeeperKeyList);
    return zookeeperKeyList;
  }
}
