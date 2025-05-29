package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig.ConfigStrings.HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.*;
import static org.apache.hadoop.ozone.om.OMConfigKeys.*;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ha.HASecurityUtils;
import org.apache.hadoop.hdds.scm.server.SCMHTTPServerConfig;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSingleLargeFileVsManySmallFiles {
  private static final String COMPONENT = "test";
  private static final String OM_CERT_SERIAL_ID = "9879877970576";

  @TempDir
  private File workDir;

  private MiniOzoneHAClusterImpl cluster;
  private FileSystem fs;
  private File testKeytab;
  private MiniKdc miniKdc;
  private OzoneConfiguration conf;
  private OzoneManager om;
  private String host;
  private File ozoneKeytab;
  private File spnegoKeytab;
  private File omKeyTab;
  private File testUserKeytab;
  private String testUserPrincipal;
  private String clusterId = UUID.randomUUID().toString();
  private String scmId = UUID.randomUUID().toString();
  private String ozonePrincipal;

  @BeforeEach
  public void init(@TempDir java.nio.file.Path tempDir) throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, "localhost");
    startMiniKdc();
    setSecureConfig();
    createCredentialsInKDC();
  }

  @Test
  public void testPerformanceLargeVsSmallFiles() throws Exception {
    conf.set("fs.o3fs.impl", "org.apache.hadoop.fs.ozone.BasicOzoneFileSystem");
    conf.set("fs.ofs.impl", "org.apache.hadoop.fs.ozone.OzoneFileSystem");

    startCluster(3);

    String volume = "vol1";
    String bucket = "buck1";

    OzoneClient client = cluster.newClient();
    client.getObjectStore().createVolume(volume);
    client.getObjectStore().getVolume(volume).createBucket(bucket);

    Path fsPath = new Path(String.format("o3fs://%s.%s.%s/",
        bucket, volume, conf.get("ozone.om.address", "om")));

    fs = fsPath.getFileSystem(conf);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation.getLoginUser().doAs((PrivilegedExceptionAction<Void>) () -> {
      long startLarge = System.nanoTime();
      try (FSDataOutputStream out = fs.create(new Path("/large-file.dat"))) {
        byte[] buffer = new byte[1024 * 1024]; // 1 MB buffer
        for (int i = 0; i < 100; i++) {
          out.write(buffer);
        }
      }
      long endLarge = System.nanoTime();

      long startSmall = System.nanoTime();
      byte[] data = new byte[1024 * 1024]; // 1MB
      for (int i = 0; i < 100; i++) {
        try (FSDataOutputStream out = fs.create(new Path("/small-files/file-" + i + ".dat"))) {
          out.write(data);
        }
      }
      long endSmall = System.nanoTime();

      System.out.printf("Large file write time: %.2f sec%n", (endLarge - startLarge) / 1e9);
      System.out.printf("Small files write time: %.2f sec%n", (endSmall - startSmall) / 1e9);

      return null;
    });

  }

  @AfterEach
  public void tearDown() {
    miniKdc.stop();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void startMiniKdc() throws Exception {
    Properties securityProperties = MiniKdc.createConf();
    miniKdc = new MiniKdc(securityProperties, workDir);
    miniKdc.start();
  }

  private void setSecureConfig() throws IOException {
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    host = InetAddress.getLocalHost().getCanonicalHostName()
        .toLowerCase();

    conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.name());

    String curUser = UserGroupInformation.getCurrentUser().getUserName();
    conf.set(OZONE_ADMINISTRATORS, curUser);

    String realm = miniKdc.getRealm();
    String hostAndRealm = host + "@" + realm;
    ozonePrincipal = "scm/" + hostAndRealm;
//    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, "scm/" + hostAndRealm);
    conf.set(HDDS_SCM_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);
//    conf.set(HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_SCM/" + hostAndRealm);
    conf.set(HDDS_SCM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_SCM/" + hostAndRealm);
//    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, "om/" + hostAndRealm);
    conf.set(OZONE_OM_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);
//    conf.set(OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_OM/" + hostAndRealm);
    conf.set(OZONE_OM_HTTP_KERBEROS_PRINCIPAL_KEY, "HTTP_OM/" + hostAndRealm);
    conf.set(HDDS_DATANODE_KERBEROS_PRINCIPAL_KEY, ozonePrincipal);

    ozoneKeytab = new File(workDir, "scm.keytab");
    spnegoKeytab = new File(workDir, "http.keytab");
    testUserKeytab = new File(workDir, "testuser.keytab");
    testUserPrincipal = "test@" + realm;

    conf.set(HDDS_SCM_KERBEROS_KEYTAB_FILE_KEY,
        ozoneKeytab.getAbsolutePath());
    conf.set(HDDS_SCM_HTTP_KERBEROS_KEYTAB_FILE_KEY,
        spnegoKeytab.getAbsolutePath());
    conf.set(OZONE_OM_KERBEROS_KEYTAB_FILE_KEY,
        ozoneKeytab.getAbsolutePath());
    conf.set(OZONE_OM_HTTP_KERBEROS_KEYTAB_FILE,
        spnegoKeytab.getAbsolutePath());
    conf.set(HDDS_DATANODE_KERBEROS_KEYTAB_FILE_KEY,
        ozoneKeytab.getAbsolutePath());

    conf.setBoolean(HADOOP_SECURITY_AUTHORIZATION, true);
//    conf.set("ozone.om.grpc.port", "0"); // Why: without this, it failed by java.io.IOException: Failed to bind to address 0.0.0.0/0.0.0.0:8981
  }

  private void startCluster(int numSCMs)
      throws IOException, TimeoutException, InterruptedException {
    OzoneManager.setTestSecureOmFlag(true);
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf)
        .setSCMServiceId("TestFS")
        .setNumOfStorageContainerManagers(numSCMs)
        .setNumOfOzoneManagers(1);

    cluster = builder.build();
    cluster.waitForClusterToBeReady();
  }

  private void createCredentialsInKDC() throws Exception {
    SCMHTTPServerConfig httpServerConfig =
        conf.getObject(SCMHTTPServerConfig.class);
    createPrincipal(ozoneKeytab, ozonePrincipal);
    createPrincipal(spnegoKeytab, httpServerConfig.getKerberosPrincipal());
    createPrincipal(testUserKeytab, testUserPrincipal);
  }

  private void createPrincipal(File keytab, String... principal)
      throws Exception {
    miniKdc.createPrincipal(keytab, principal);
  }
}
