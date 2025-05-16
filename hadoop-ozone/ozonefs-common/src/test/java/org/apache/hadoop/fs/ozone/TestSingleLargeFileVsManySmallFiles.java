package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;

public class TestSingleLargeFileVsManySmallFiles {
  private MiniOzoneCluster cluster;
  private FileSystem fs;
  private File testKeytab;
  private MiniKdc kdc;

  @BeforeEach
  public void init(@TempDir java.nio.file.Path tempDir) throws Exception {
    startMiniKdc();
    OzoneConfiguration conf = new OzoneConfiguration();
    configKerberos(conf);

    conf.set("fs.o3fs.impl", "org.apache.hadoop.fs.ozone.BasicOzoneFileSystem");
    conf.set("fs.ofs.impl", "org.apache.hadoop.fs.ozone.OzoneFileSystem"); // optional, if you test OFS too

    conf.set(OZONE_METADATA_DIRS, tempDir.toString());
    System.out.println("Before build cluster:");
    conf.writeXml(System.out);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    System.out.println("After the build:");
    conf.writeXml(System.out);
    String volume = "vol1";
    String bucket = "buck1";

    OzoneClient client = cluster.newClient();
    client.getObjectStore().createVolume(volume);
    client.getObjectStore().getVolume(volume).createBucket(bucket);

    Path fsPath = new Path(String.format("o3fs://%s.%s.%s/",
        bucket, volume, conf.get("ozone.om.address", "om")));

    fs = fsPath.getFileSystem(conf);
  }

  @Test
  public void testPerformanceLargeVsSmallFiles() throws Exception {
    UserGroupInformation.getLoginUser().doAs((PrivilegedExceptionAction<Void>) () -> {
      // Write single large file (e.g. 100MB)
      long startLarge = System.nanoTime();
      try (FSDataOutputStream out = fs.create(new Path("/large-file.dat"))) {
        byte[] buffer = new byte[1024 * 1024]; // 1 MB buffer
        for (int i = 0; i < 100; i++) {
          out.write(buffer);
        }
      }
      long endLarge = System.nanoTime();

      // Write 100 small files (1MB each)
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
    if (cluster != null) {
      cluster.shutdown();
      kdc.stop();
    }
  }

  private void startMiniKdc() throws Exception {
    File kdcBaseDir = Files.createTempDirectory("kdc").toFile();
    Properties kdcConf = MiniKdc.createConf();

    kdc = new MiniKdc(kdcConf, kdcBaseDir);
    kdc.start();

// Create keytabs for OM, SCM, DN, and test user
    testKeytab = new File(kdcBaseDir, "test.keytab");
    kdc.createPrincipal(testKeytab,
        "testuser",
        "om/localhost",
        "scm/localhost",
        "dn/localhost"
    );
  }

  private void configKerberos(OzoneConfiguration conf) throws IOException {
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("ozone.security.enabled", "true");
    conf.set("ozone.security.authentication.type", "kerberos");

    conf.set("ozone.om.kerberos.principal", "om/localhost@EXAMPLE.COM");
    conf.set("ozone.om.kerberos.keytab.file", testKeytab.getAbsolutePath());

    conf.set("ozone.scm.kerberos.principal", "scm/localhost@EXAMPLE.COM");
    conf.set("ozone.scm.kerberos.keytab.file", testKeytab.getAbsolutePath());

    conf.set("ozone.dn.kerberos.principal", "dn/localhost@EXAMPLE.COM");
    conf.set("ozone.dn.kerberos.keytab.file", testKeytab.getAbsolutePath());

    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab("testuser@EXAMPLE.COM", testKeytab.getAbsolutePath());
  }
}
