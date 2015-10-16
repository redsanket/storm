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

import backtype.storm.nimbus.NimbusInfo;
import backtype.storm.utils.Utils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The class is responsible for getting all the list of keys for active topologies
 */
public class BlobDownloader {
  private CuratorFramework zkClient;
  private static final Logger LOG = LoggerFactory.getLogger(BlobDownloader.class);

  // Handle cases for download not possible or replication related stuff here
  public void downloadBlobs(BlobStore blobStore, List<String> keys, Map conf) throws Exception {
    zkClient = Utils.createZKClient(conf);
    List<NimbusInfo> nimbusInfoList = new ArrayList<>();
    for(String key: keys) {
      nimbusInfoList = Utils.getNimbodesWithLatestVersionOfBlob(zkClient, key);
      Utils.downloadBlob(conf, blobStore, key, nimbusInfoList);
    }
  }
}
