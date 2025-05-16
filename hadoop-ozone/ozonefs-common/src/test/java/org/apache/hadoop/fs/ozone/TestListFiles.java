package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;

public class TestListFiles {
  private MiniOzoneCluster cluster;
  private FileSystem fs;

  @BeforeEach
  public void init(@TempDir java.nio.file.Path tempDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("fs.o3fs.impl", "org.apache.hadoop.fs.ozone.BasicOzoneFileSystem");
    conf.set("fs.ofs.impl", "org.apache.hadoop.fs.ozone.OzoneFileSystem"); // optional, if you test OFS too

    conf.set(OZONE_METADATA_DIRS, tempDir.toString());

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();

    String volume = "vol1";
    String bucket = "buck1";

    OzoneClient client = cluster.newClient();
    client.getObjectStore().createVolume(volume);
    client.getObjectStore().getVolume(volume).createBucket(bucket);

    Path fsPath = new Path(String.format("o3fs://%s.%s.%s/",
        bucket, volume, conf.get("ozone.om.address", "om")));

    fs = fsPath.getFileSystem(conf);
    Path filePath = new Path("/dir1/file1.txt");
    try (FSDataOutputStream out = fs.create(filePath)) {
      out.writeUTF("Hello from FileSystem!");
    }
  }

  @Test
  public void testListFiles() throws IOException {
    Path root = new Path("/");
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(root, true);
    System.out.println(files.hasNext());

    while (files.hasNext()) {
      System.out.println(files.next());
      System.out.println();
    }
  }

  @AfterEach
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
