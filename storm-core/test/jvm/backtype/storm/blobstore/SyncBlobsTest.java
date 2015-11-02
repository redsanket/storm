package backtype.storm.blobstore;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import static org.junit.Assert.assertTrue;

public class SyncBlobsTest {

  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreTest.class);
  protected static MiniDFSCluster dfscluster = null;
  protected static Configuration hadoopConf = null;
  URI base;
  File baseFile;
  private static Map conf = new HashMap();

  @Before
  public void init() {
    initializeConfigs();
    baseFile = new File("/tmp/blob-store-test-"+ UUID.randomUUID());
    base = baseFile.toURI();
  }

  @After
  public void cleanUp() throws IOException {
    FileUtils.deleteDirectory(baseFile);
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
    ArrayList<String> zookeeper_list = new ArrayList<>();
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
}
