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

import backtype.storm.utils.ZookeeperAuthInfo;

import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import static backtype.storm.utils.Utils.newCurator;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanBlobState implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(SyncBlob.class);
  private CuratorFramework zkClient;
  private BlobStore blobStore;
  private Map conf;

  @Override
  public void run() {
    try {
      Thread.sleep(300);
      while (true) {
        cleanDeletedBlobState();
        Thread.sleep(300);
      }
    } catch (InterruptedException exp) {

    }
  }

  public CleanBlobState(BlobStore blobStore, Map conf, CuratorFramework zkClient) {
    this.blobStore = blobStore;
    this.zkClient = zkClient;
  }

  public void cleanDeletedBlobState() {
    try {
      List<String> keyListZookeeper = zkClient.getChildren().forPath("/blobstore");
      int deletedCount = 0;
      for(String key : keyListZookeeper) {
        List<String> stateInfo = zkClient.getChildren().forPath("/blobstore/" + key);
        for(String state : stateInfo) {
          String[] statusInfo = state.split(":");
          if(statusInfo != null && statusInfo.length == 3) {
            deletedCount++;
          }
        }
        if(stateInfo !=null && stateInfo.size() == deletedCount) {
          zkClient.delete().deletingChildrenIfNeeded().forPath("/blobstore/" + key);
        }
        deletedCount = 0;
      }
    } catch (Exception exp) {

    }
  }
}
