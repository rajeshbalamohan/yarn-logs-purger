/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * <pre>
 * Very basic command line tool to purge yarn aggregated logs older than certain days
 *
 * Usage:
 *
 * 1. List: yarn jar ./target/yarn-logs-purger-1.0-SNAPSHOT.jar -DdeleteOlderThan=300
 *
 * 2. List & purge: yarn jar ./target/yarn-logs-purger-1.0-SNAPSHOT.jar -DdeleteOlderThan=300 -DdeleteFiles=true
 *
 * </pre>
 */
public class AggregatedLogsPurger extends Configured implements Tool {

  private Configuration conf;
  private int deleteOlderThanDays;

  private String suffix;
  private Path rootLogDir;

  private boolean shouldDelete;

  public boolean purge() throws IOException {
    LocalDateTime now = LocalDateTime.now();
    LocalDateTime deleteLogsOlderThanTime = now.minusDays(deleteOlderThanDays);

    //Identify which log dirs should be deleted
    FileSystem fs = rootLogDir.getFileSystem(conf);
    try {

      long totalBytes = 0;
      for (FileStatus userDir : fs.listStatus(rootLogDir)) {
        if (userDir.isDirectory()) {
          Path userDirPath = new Path(userDir.getPath(), suffix);
          System.out.println("Checking for userDir : " + userDirPath);
          for (FileStatus appDir : fs.listStatus(userDirPath)) {
            LocalDateTime appDirDate = getAppDirDateTime(appDir.getModificationTime());
            if (appDirDate.isBefore(deleteLogsOlderThanTime)) {
              long size = getLengthRecursively(fs, appDir.getPath());
              System.out.println(appDir.getPath() + ", " + appDir.getOwner()
                  + ", " + appDirDate.toString() + ", size=" + size);
              totalBytes += size;
              if (shouldDelete) {
                System.out.println("Deleting " + appDir.getPath());
                fs.delete(appDir.getPath(), true);
              }
            }
          }
        }
      }
      System.out.println("Savings : " + totalBytes);
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    } finally {
      fs.close();
    }
    return true;
  }

  private long getLengthRecursively(FileSystem fs, Path path) throws IOException {
    long size = 0;
    for (FileStatus status : fs.listStatus(path)) {
      if (status.isDirectory()) {
        getLengthRecursively(fs, status.getPath());
      } else {
        size += status.getLen();
      }
    }
    return size;
  }

  private LocalDateTime getAppDirDateTime(long appDirModificationTime) {
    return LocalDateTime
        .ofInstant(Instant.ofEpochMilli(appDirModificationTime), ZoneId.systemDefault());
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new YarnConfiguration(), new AggregatedLogsPurger(), args);
  }

  @Override public int run(String[] args) throws Exception {
    this.conf = getConf();
    this.deleteOlderThanDays = getConf().getInt("deleteOlderThan", 0);
    Preconditions.checkArgument(deleteOlderThanDays > 1, "Usage: yarn jar "
        + "./target/yarn-logs-purger-1.0-SNAPSHOT.jar -DdeleteOlderThan=300 "
        + "-DdeleteFiles=true.  Please provide valid argument for deleteOlderThanDays. It has to "
        + "be > 0");
    this.shouldDelete = getConf().getBoolean("deleteFiles", false);

    this.suffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(conf);
    this.rootLogDir = new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    return (purge()) ? 0 : -1;
  }

}
