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

import static backtype.storm.blobstore.BlobStoreAclHandler.ADMIN;
import static backtype.storm.blobstore.BlobStoreAclHandler.READ;
import static backtype.storm.blobstore.BlobStoreAclHandler.WRITE;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import javax.security.auth.Subject;

import backtype.storm.generated.*;
import backtype.storm.utils.Utils;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

/**
 * Provides a local file system backed blob store implementation for Nimbus.
 */
public class LocalFsBlobStore extends BlobStore {
  public static final Logger LOG = LoggerFactory.getLogger(LocalFsBlobStore.class);
  private static final String DATA_PREFIX = "data_";
  private static final String META_PREFIX = "meta_";
  protected BlobStoreAclHandler _aclHandler;
  private FileBlobStoreImpl fbs;

  @Override
  public void prepare(Map conf, String overrideBase) {
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
    //Empty
  }

  @Override
  public BlobReplication getBlobReplication(String key, Subject who) {
    return new BlobReplication(1);
  }

  @Override
  public BlobReplication updateBlobReplication(String key, int replication, Subject who) {
    return new BlobReplication(1);
  }

  public void fullCleanup(long age) throws IOException {
    fbs.fullCleanup(age);
  }
}
