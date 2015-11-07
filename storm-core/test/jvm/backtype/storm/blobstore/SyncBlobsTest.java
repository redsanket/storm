package backtype.storm.blobstore;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.HashMap;
import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

public class SyncBlobsTest {

  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreTest.class);
  URI base;
  File baseFile;
  private static Map conf = new HashMap();
  private NIOServerCnxnFactory factory;
  int port = 0;
  CuratorFramework zkClient;
  ZooKeeperServer zkServer;

  @Before
  public void init() throws IOException {
    initializeConfigs();
    baseFile = new File("/tmp/blob-store-test-"+ UUID.randomUUID());
    base = baseFile.toURI();
  }

  @After
  public void cleanUp() throws IOException {
    FileUtils.deleteDirectory(baseFile);
    if (factory != null) {
      factory.shutdown();
    }
    if (zkClient != null) {
      zkClient.close();
    }
  }

  // Method which initializes nimbus admin
  public static void initializeConfigs() {
    conf.put(Config.NIMBUS_ADMINS,"admin");
    conf.put(Config.NIMBUS_SUPERVISOR_USERS,"supervisor");
  }

  private LocalFsBlobStore initLocalFs() {
    LocalFsBlobStore store = new LocalFsBlobStore();
    Map conf = Utils.readStormConfig();
    conf.put(Config.STORM_LOCAL_DIR, baseFile.getAbsolutePath());
    conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN,"backtype.storm.security.auth.DefaultPrincipalToLocal");
    this.conf = conf;
    store.prepare(conf, null, null);
    return store;
  }

  @Test
  public void testSyncBlobsForKeysToDownload() {
    BlobStore store = initLocalFs();
    SyncBlobs sync = new SyncBlobs(store, conf);
    // test for keylist to download
    List<String> zkList = new ArrayList<String>();
    zkList.add("key1");
    List<String> blobStoreList = new ArrayList<String>();
    blobStoreList.add("key1");
    List<String> resultList = sync.getKeyListToDownload(blobStoreList,zkList);
    assertTrue("Not Empty", resultList.isEmpty());
    zkList.add("key1");
    blobStoreList.add("key2");
    resultList =  sync.getKeyListToDownload(blobStoreList,zkList);
    assertTrue("Not Empty", resultList.isEmpty());
    blobStoreList.remove("key1");
    blobStoreList.remove("key2");
    zkList.add("key1");
    resultList =  sync.getKeyListToDownload(blobStoreList,zkList);
    assertTrue("Unexpected keys to download", (resultList.size() == 1) && (resultList.contains("key1")));
  }

  @Test
  public void testGetLatestVersion() throws Exception {
    List<String> stateInfoList = new ArrayList<String>();
    stateInfoList.add("nimbus1:8000-123789434");
    stateInfoList.add("nimbus-1:8000-123789435");
    assertTrue("Failed to get the latest version", Utils.getLatestVersion(stateInfoList)==123789435);
  }
}
