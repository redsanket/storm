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

import backtype.storm.Config;
import backtype.storm.generated.SettableBlobMeta;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.KeyAlreadyExistsException;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.generated.ReadableBlobMeta;

import backtype.storm.nimbus.NimbusInfo;
import backtype.storm.utils.Utils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static backtype.storm.blobstore.BlobStoreAclHandler.ADMIN;
import static backtype.storm.blobstore.BlobStoreAclHandler.READ;
import static backtype.storm.blobstore.BlobStoreAclHandler.WRITE;

/**
 * Provides a local file system backed blob store implementation for Nimbus.
 */
public class LocalFsBlobStore extends BlobStore {
  public static final Logger LOG = LoggerFactory.getLogger(LocalFsBlobStore.class);
  private static final String DATA_PREFIX = "data_";
  private static final String META_PREFIX = "meta_";
  protected BlobStoreAclHandler _aclHandler;
  private final String BLOBSTORE_SUBTREE = "/blobstore";
  private NimbusInfo nimbusInfo;
  private FileBlobStoreImpl fbs;
  private Map conf;

  @Override
  public void prepare(Map conf, String overrideBase, NimbusInfo nimbusInfo) {
    this.conf = conf;
    this.nimbusInfo = nimbusInfo;
     if (overrideBase == null) {
      overrideBase = (String)conf.get(Config.BLOBSTORE_DIR);
      if (overrideBase == null) {
        overrideBase = (String) conf.get(Config.STORM_LOCAL_DIR);
      }
    }
    File baseDir = new File(overrideBase, BASE_BLOBS_DIR_NAME);
    try {
      fbs = new FileBlobStoreImpl(baseDir, conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    _aclHandler = new BlobStoreAclHandler(conf);
  }

  @Override
  public AtomicOutputStream createBlob(String key, SettableBlobMeta meta, Subject who) throws AuthorizationException, KeyAlreadyExistsException {
    LOG.debug("Creating Blob for key {}", key);
    validateKey(key);
    _aclHandler.normalizeSettableBlobMeta(key, meta, who, READ | WRITE | ADMIN);
    BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
    _aclHandler.validateACL(meta.get_acl(), READ | WRITE | ADMIN, who, key);
    if (fbs.exists(DATA_PREFIX+key)) {
      throw new KeyAlreadyExistsException(key);
    }
    BlobStoreFileOutputStream mOut = null;
    try {
      mOut = new BlobStoreFileOutputStream(fbs.write(META_PREFIX+key, true));
      mOut.write(Utils.thriftSerialize((TBase) meta));
      mOut.close();
      mOut = null;
      return new BlobStoreFileOutputStream(fbs.write(DATA_PREFIX+key, true));
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (mOut != null) {
        try {
          mOut.cancel();
        } catch (IOException e) {
          //Ignored
        }
      }
    }
  }

  @Override
  public AtomicOutputStream updateBlob(String key, Subject who) throws AuthorizationException, KeyNotFoundException {
    checkForBlobOrDownload(key);
    SettableBlobMeta meta = getStoredBlobMeta(key);
    _aclHandler.validateACL(meta.get_acl(), WRITE, who, key);
    validateKey(key);
    try {
      return new BlobStoreFileOutputStream(fbs.write(DATA_PREFIX+key, false));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private SettableBlobMeta getStoredBlobMeta(String key) throws KeyNotFoundException {
    InputStream in = null;
    try {
      LocalFsBlobStoreFile pf = fbs.read(META_PREFIX+key);
      try {
        in = pf.getInputStream();
      } catch (FileNotFoundException fnf) {
        throw new KeyNotFoundException(key);
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte [] buffer = new byte[2048];
      int len;
      while ((len = in.read(buffer)) > 0) {
        out.write(buffer, 0, len);
      }
      in.close();
      in = null;
      SettableBlobMeta sbm = Utils.thriftDeserialize(SettableBlobMeta.class, out.toByteArray());
      return sbm;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          //Ignored
        }
      }
    }
  }

  @Override
  public ReadableBlobMeta getBlobMeta(String key, Subject who) throws AuthorizationException, KeyNotFoundException {
    if(!checkForBlobOrDownload(key)) {
      checkForBlobUpdate(key);
    }
    validateKey(key);
    SettableBlobMeta meta = getStoredBlobMeta(key);
    _aclHandler.validateUserCanReadMeta(meta.get_acl(), who, key);
    ReadableBlobMeta rbm = new ReadableBlobMeta();
    rbm.set_settable(meta);
    try {
      LocalFsBlobStoreFile pf = fbs.read(DATA_PREFIX+key);
      rbm.set_version(pf.getModTime());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return rbm;
  }

  @Override
  public void setBlobMeta(String key, SettableBlobMeta meta, Subject who) throws AuthorizationException, KeyNotFoundException {
    checkForBlobOrDownload(key);
    validateKey(key);
    _aclHandler.normalizeSettableBlobMeta(key, meta, who, ADMIN);
    BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
    SettableBlobMeta orig = getStoredBlobMeta(key);
    _aclHandler.validateACL(orig.get_acl(), ADMIN, who, key);
    BlobStoreFileOutputStream mOut = null;
    try {
      mOut = new BlobStoreFileOutputStream(fbs.write(META_PREFIX+key, false));
      mOut.write(Utils.thriftSerialize((TBase) meta));
      mOut.close();
      mOut = null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (mOut != null) {
        try {
          mOut.cancel();
        } catch (IOException e) {
          //Ignored
        }
      }
    }
  }

  @Override
  public void deleteBlob(String key, Subject who) throws AuthorizationException, KeyNotFoundException {
    checkForBlobOrDownload(key);
    validateKey(key);
    SettableBlobMeta meta = getStoredBlobMeta(key);
    _aclHandler.validateACL(meta.get_acl(), WRITE, who, key);
    try {
      fbs.deleteKey(DATA_PREFIX+key);
      fbs.deleteKey(META_PREFIX+key);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public InputStreamWithMeta getBlob(String key, Subject who) throws AuthorizationException, KeyNotFoundException {
    if(!checkForBlobOrDownload(key)) {
      checkForBlobUpdate(key);
    }
    validateKey(key);
    SettableBlobMeta meta = getStoredBlobMeta(key);
    _aclHandler.validateACL(meta.get_acl(), READ, who, key);
    try {
      return new BlobStoreFileInputStream(fbs.read(DATA_PREFIX+key));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<String> listKeys(Subject who) {
    try {
      return new KeyTranslationIterator(fbs.listKeys(), DATA_PREFIX);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {
  }

  @Override
  public int getBlobReplication(String key, Subject who) throws Exception {
    CuratorFramework zkClient = Utils.createZKClient(conf);
    if (zkClient.checkExists().forPath("/blobstore/" + key) == null) {
       zkClient.close();
       return 0;
    }
    int replicationCount = zkClient.getChildren().forPath("/blobstore/" + key).size();
    zkClient.close();
    return replicationCount;
  }

  @Override
  public int updateBlobReplication(String key, int replication, Subject who) {
    int replicationCount = 0;
    try {
      LOG.warn ("For local file system blob store the update blobs function does not work." +
              "Please use HDFS blob store to make this feature available. The replication your " +
              "are noticing is the present replication of the blob based on its availability on various nimbuses");
      replicationCount = this.getBlobReplication(key, who);
    } catch (Exception e) {
      LOG.error("Exception {}", e);
    }
    return replicationCount;
  }

  //This additional check and download is for nimbus high availability in case you have more than one nimbus
  public boolean checkForBlobOrDownload(String key) {
    boolean checkBlobDownload = false;
    try {
      List<String> keyList = Utils.getKeyListFromBlobStore(this);
      if (!keyList.contains(key)) {
        CuratorFramework zkClient = Utils.createZKClient(conf);
        if (zkClient.checkExists().forPath("/blobstore/" + key) != null) {
          List<NimbusInfo> nimbusList = Utils.getNimbodesWithLatestVersionOfBlob(zkClient, key);
          if (Utils.downloadMissingBlob(conf, this, key, nimbusList)) {
            LOG.debug("Updating blobs state");
            Utils.createStateInZookeeper(conf, key, nimbusInfo);
            checkBlobDownload = true;
          }
        }
        zkClient.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return checkBlobDownload;
  }

  public void checkForBlobUpdate(String key) {
    CuratorFramework zkClient = Utils.createZKClient(conf);
    Utils.updateKeyForBlobStore(conf, this, zkClient, key, nimbusInfo);
    zkClient.close();
  }

  public void fullCleanup(long age) throws IOException {
    fbs.fullCleanup(age);
  }
}
