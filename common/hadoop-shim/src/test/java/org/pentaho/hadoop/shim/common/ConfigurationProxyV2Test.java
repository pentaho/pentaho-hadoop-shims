/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * test checks whether hbase-site.xml, hive-site.xml, hdfs-site.xml from classpath are
 * added for hadoop configuration, according test files with hadoop properties are in resource
 * folder for test
 *
 * Created by Vasilina_Terehova on 4/12/2017.
 */
public class ConfigurationProxyV2Test {

  @Test
  public void checkConfigsInConfigurationAddedHbaseSiteXml() throws IOException {
    ConfigurationProxyV2 configurationProxyV2 = new ConfigurationProxyV2();
    System.out.println( configurationProxyV2.getJobConf() );
    assertEquals( "15", configurationProxyV2.get( "hbase.client.primaryCallTimeout.get" ) );
    assertEquals( "16", configurationProxyV2.get( "hbase.client.primaryCallTimeout.multiget" ) );
  }

  @Test
  public void checkConfigsInConfigurationAddedHDFSSiteXml() throws IOException {
    ConfigurationProxyV2 configurationProxyV2 = new ConfigurationProxyV2();
    System.out.println( configurationProxyV2.getJobConf() );
    assertEquals( "true", configurationProxyV2.get( "hive.optimize.bucketmapjoin.sortedmerge" ) );
    assertEquals( "11000", configurationProxyV2.get( "hive.smbjoin.cache.rows" ) );
  }

  @Test
  public void checkConfigsInConfigurationAddedHiveSiteXml() throws IOException {
    ConfigurationProxyV2 configurationProxyV2 = new ConfigurationProxyV2();
    System.out.println( configurationProxyV2.getJobConf() );
    assertEquals( "4", configurationProxyV2.get( "dfs.replication" ) );
    assertEquals( "true", configurationProxyV2.get( "dfs.client.domain.socket.data.traffic" ) );
  }
}
