/*! ******************************************************************************
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
package org.pentaho.hadoop.shim.common;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.hadoop.mapreduce.YarnQueueAclsException;
import org.pentaho.hadoop.mapreduce.YarnQueueAclsVerifier;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

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

  @Test(expected = YarnQueueAclsException.class)
  public void testSubmitWhenUserHasNoPermissionsToSubmitJobInQueueShouldRaiseYarnQueueAclsException() throws IOException, InterruptedException, ClassNotFoundException{
    Mockito.spy( YarnQueueAclsVerifier.class );
    ConfigurationProxyV2 configurationProxyV2 = Mockito.mock( ConfigurationProxyV2.class );
    Cluster cluster = Mockito.mock( Cluster.class );
    Job job = Mockito.mock( Job.class );

    Mockito.when( configurationProxyV2.getJob() ).thenReturn( job );
    Mockito.when( configurationProxyV2.createClusterDescription( Mockito.any( Configuration.class ) ) ).thenReturn( cluster );
    Mockito.when( configurationProxyV2.submit() ).thenCallRealMethod();
    Mockito.when( cluster.getQueueAclsForCurrentUser() ).thenReturn( new QueueAclsInfo[]{
      new QueueAclsInfo( StringUtils.EMPTY, new String[]{
        "ANOTHER_RIGHTS"
      } ),
      new QueueAclsInfo( StringUtils.EMPTY, new String[]{})
    });

    configurationProxyV2.submit();
  }

  @Test
  public void testSubmitWhenUserHasPermissionsToSubmitJobInQueueShouldExecuteSuccessfully() throws IOException, InterruptedException, ClassNotFoundException{
    Mockito.spy( YarnQueueAclsVerifier.class );
    ConfigurationProxyV2 configurationProxyV2 = Mockito.mock( ConfigurationProxyV2.class );
    Cluster cluster = Mockito.mock( Cluster.class );
    Job job = Mockito.mock( Job.class );

    Mockito.when( configurationProxyV2.getJob() ).thenReturn( job );
    Mockito.when( configurationProxyV2.createClusterDescription( Mockito.any( Configuration.class ) ) ).thenReturn( cluster );
    Mockito.when( configurationProxyV2.submit() ).thenCallRealMethod();
    Mockito.when( cluster.getQueueAclsForCurrentUser() ).thenReturn( new QueueAclsInfo[]{
      new QueueAclsInfo( StringUtils.EMPTY, new String[]{
        "SUBMIT_APPLICATIONS"
      } ),
      new QueueAclsInfo( StringUtils.EMPTY, new String[]{})
    });

    Assert.assertNotNull( configurationProxyV2.submit() );
  }
}
