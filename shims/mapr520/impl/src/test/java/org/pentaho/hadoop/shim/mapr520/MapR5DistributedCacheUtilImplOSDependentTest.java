/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.hadoop.shim.mapr520;

import static org.junit.Assume.assumeTrue;

import java.io.IOException;

import org.pentaho.hadoop.shim.common.DistributedCacheUtilImplOSDependentTest;

/**
 * These tests are skipped because of having issue with setting permissions on hadoop local file system for mapr on
 * Windows.
 *
 * @see <a href=
 *      "http://jira.pentaho.com/browse/BAD-601?focusedCommentId=294386&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-294386">BAD-601#comment-294386</a>
 *      for more details.
 */
public class MapR5DistributedCacheUtilImplOSDependentTest extends DistributedCacheUtilImplOSDependentTest {

  @Override
  public void stageForCache() throws Exception {
    // Don't run this test on Windows env
    assumeTrue( !isWindows() );
    super.stageForCache();
  }

  @Override
  public void stageForCache_destination_exists() throws Exception {
    // Don't run this test on Windows env
    assumeTrue( !isWindows() );
    super.stageForCache_destination_exists();
  }

  @Override
  public void stagePluginsForCache() throws Exception {
    // Don't run this test on Windows env
    assumeTrue( !isWindows() );
    super.stagePluginsForCache();
  }

  @Override
  public void findFiles_hdfs_native() throws Exception {
    // Don't run this test on Windows env
    assumeTrue( !isWindows() );
    super.findFiles_hdfs_native();
  }

  @Override
  public void installKettleEnvironment() throws Exception {
    // Don't run this test on Windows env
    assumeTrue( !isWindows() );
    super.installKettleEnvironment();
  }

  @Override
  public void installKettleEnvironment_additional_plugins() throws Exception {
    // Don't run this test on Windows env
    assumeTrue( !isWindows() );
    super.installKettleEnvironment_additional_plugins();
  }

  @Override
  public void isPmrInstalledAt() throws IOException {
    // Don't run this test on Windows env
    assumeTrue( !isWindows() );
    super.isPmrInstalledAt();
  }

  @Override
  public void configureWithPmr() throws Exception {
    // Don't run this test on Windows env
    assumeTrue( !isWindows() );
    super.configureWithPmr();
  }

}
