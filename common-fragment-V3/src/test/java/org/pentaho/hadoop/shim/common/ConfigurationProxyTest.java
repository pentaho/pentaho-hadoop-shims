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

package org.pentaho.hadoop.shim.common;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MockQueueAclsInfo;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Progressable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.hadoop.mapreduce.YarnQueueAclsException;
import org.pentaho.hadoop.mapreduce.YarnQueueAclsVerifier;
import org.pentaho.hadoop.shim.common.fs.PathProxy;

import java.io.IOException;

import static org.junit.Assert.*;

public class ConfigurationProxyTest {

  private ConfigurationProxy configurationProxy;

  @Before
  public void setUp() throws Exception {
    configurationProxy = new ConfigurationProxy();
  }

  @Test
  public void testSetMapperClass() throws Exception {
    configurationProxy.setMapperClass( Mapper.class );
    assertEquals( Mapper.class, configurationProxy.getMapperClass() );
  }

  @Test
  public void testSetCombinerClass() throws Exception {
    configurationProxy.setCombinerClass( Reducer.class );
    assertEquals( Reducer.class, configurationProxy.getCombinerClass() );
  }

  @Test
  public void testSetReducerClass() throws Exception {
    configurationProxy.setReducerClass( Reducer.class );
    assertEquals( Reducer.class, configurationProxy.getReducerClass() );
  }

  @Test
  public void testSetMapRunnerClass() throws Exception {
    configurationProxy.setMapRunnerClass( MapRunner.class );
    assertEquals( MapRunner.class, configurationProxy.getMapRunnerClass() );
  }

  @Test
  public void testSetInputFormat() throws Exception {
    configurationProxy.setInputFormat( TestInputFormat.class );
    assertEquals( TestInputFormat.class, configurationProxy.getInputFormat().getClass() );
  }

  @Test
  public void testSetOutputFormat() throws Exception {
    configurationProxy.setOutputFormat( TestOutputFormat.class );
    assertEquals( TestOutputFormat.class, configurationProxy.getOutputFormat().getClass() );
  }

  @Test
  public void testGetDefaultFileSystemURL() throws Exception {
    String defaultFileSystemURL = configurationProxy.getDefaultFileSystemURL();
    assertEquals( defaultFileSystemURL, configurationProxy.get( "fs.default.name", "" ) );
  }

  @Test
  public void testGetAsDelegateConf() throws Exception {
    assertNull( configurationProxy.getAsDelegateConf( String.class ) );
    assertEquals( configurationProxy, configurationProxy.getAsDelegateConf( ConfigurationProxy.class ) );
  }

  @Test
  public void testSetInputPaths() throws Exception {
    configurationProxy.setInputPaths( null );
    Path[] inputPaths = FileInputFormat.getInputPaths( configurationProxy );
    assertEquals( 0, inputPaths.length );

    PathProxy path1 = new PathProxy( "file://path1" );
    PathProxy path2 = new PathProxy( "file://path2" );
    configurationProxy.setInputPaths( path1, path2 );

    inputPaths = FileInputFormat.getInputPaths( configurationProxy );
    assertEquals( 2, inputPaths.length );
    assertArrayEquals( new Path[] {path1, path2}, inputPaths );
  }

  @Test
  public void testSetOutputPath() throws Exception {
    PathProxy path = new PathProxy( "file://output" );
    configurationProxy.setOutputPath( path );
    assertEquals( path, FileOutputFormat.getOutputPath( configurationProxy ) );
  }

  @Test(expected = YarnQueueAclsException.class)
  public void testSubmitWhenUserHasNoPermissionsToSubmitJobInQueueShouldRaiseYarnQueueAclsException() throws IOException, InterruptedException, ClassNotFoundException{
    Mockito.spy( YarnQueueAclsVerifier.class );
    ConfigurationProxy configurationProxy = Mockito.mock( ConfigurationProxy.class );
    JobClient jobClient = Mockito.mock( JobClient.class );

    Mockito.when( configurationProxy.createJobClient() ).thenReturn( jobClient );
    Mockito.when( configurationProxy.submit() ).thenCallRealMethod();
    Mockito.when( jobClient.getQueueAclsForCurrentUser() ).thenReturn( new MockQueueAclsInfo[]{
      new MockQueueAclsInfo( StringUtils.EMPTY, new String[]{
        "ANOTHER_RIGHTS"
      } ),
      new MockQueueAclsInfo( StringUtils.EMPTY, new String[]{})
    } );

    configurationProxy.submit();
  }

  @Test
  public void testSubmitWhenUserHasPermissionsToSubmitJobInQueueShouldExecuteSuccessfully() throws IOException, InterruptedException, ClassNotFoundException{
    Mockito.spy( YarnQueueAclsVerifier.class );
    ConfigurationProxy configurationProxy = Mockito.mock( ConfigurationProxy.class );
    JobClient jobClient = Mockito.mock( JobClient.class );
    RunningJob runningJob = Mockito.mock( RunningJob.class );

    Mockito.when( configurationProxy.createJobClient() ).thenReturn( jobClient );
    Mockito.when( configurationProxy.submit() ).thenCallRealMethod();
    Mockito.when( jobClient.getQueueAclsForCurrentUser() ).thenReturn( new MockQueueAclsInfo[]{
      new MockQueueAclsInfo( StringUtils.EMPTY, new String[]{
        "SUBMIT_APPLICATIONS"
      } ),
      new MockQueueAclsInfo( StringUtils.EMPTY, new String[]{})
    } );
    Mockito.when( jobClient.submitJob( Mockito.any( JobConf.class ) ) ).thenReturn( runningJob );

    Assert.assertNotNull( configurationProxy.submit() );
  }

  static class TestInputFormat implements InputFormat {
    @Override
    public InputSplit[] getSplits( JobConf jobConf, int i ) throws IOException {
      return new InputSplit[0];
    }

    @Override
    public RecordReader getRecordReader( InputSplit inputSplit, JobConf jobConf, Reporter reporter )
        throws IOException {
      return null;
    }
  }

  static class TestOutputFormat implements OutputFormat {

    @Override
    public RecordWriter getRecordWriter( FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable )
        throws IOException {
      return null;
    }

    @Override
    public void checkOutputSpecs( FileSystem fileSystem, JobConf jobConf ) throws IOException {

    }
  }
}
