/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.pentaho.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransMeta;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * @author Tatsiana_Kasiankova
 */
@SuppressWarnings( { "unchecked", "rawtypes" } )
public class PentahoMapRunnableTest {
  private static final String WORD_TO_COUNT_TEMPLATE = "word";
  private static final String INTERNAL_HADOOP_NODE_NUMBER = "Internal.Hadoop.NodeNumber";
  private static final String MAPRED_TASK_ID = "mapred.task.id";
  private static final String MAP_TRANS_META_NAME = "Map transformation";

  /**
   * Mock trans configuration: empty trans meta with name and empty trans execution configuration
   */
  private static TransConfiguration combinerTransExecutionConfig;
  /**
   * We expect 5 log channels per run. The total should never grow past logChannelsBefore + 5.
   */
  final int EXPECTED_CHANNELS_PER_RUN = 5;
  /**
   * Run the reducer this many times
   */
  final int RUNS = 10;
  private Reporter reporterMock = mock( Reporter.class );

  private PentahoMapRunnable mapRunnable;
  private JobConf mrJobConfig;
  private TransMeta transMeta;
  private MockOutputCollector outputCollectorMock = new MockOutputCollector();
  private MockRecordReader reader;

  @BeforeClass
  public static void before() throws KettleException {
    KettleEnvironment.init();
    combinerTransExecutionConfig = MRTestUtil.getTransExecConfig( MRTestUtil.getTransMeta( MAP_TRANS_META_NAME ) );
  }

  @Before
  public void setUp() throws KettleException, IOException {
    mapRunnable = new PentahoMapRunnable();
    mrJobConfig = new JobConf();
    //Turn off all debug messages from PentahoMapRunnable to reduce unit test logs
    mrJobConfig.set( "logLevel", LogLevel.ERROR.name() );
  }

  @Test
  public void testTaskIdExtraction() throws Exception {
    mrJobConfig.set( MRTestUtil.TRANSFORMATION_MAP_XML, combinerTransExecutionConfig.getXML() );
    mrJobConfig.set( MAPRED_TASK_ID, "job_201208090841_0133" );
    mapRunnable.configure( mrJobConfig );

    String actualVariable = mapRunnable.variableSpace.getVariable( INTERNAL_HADOOP_NODE_NUMBER );
    assertEquals( "133", actualVariable );
  }

  @Test
  public void testTaskIdExtraction_over_10000() throws Exception {
    mrJobConfig.set( MRTestUtil.TRANSFORMATION_MAP_XML, combinerTransExecutionConfig.getXML() );
    mrJobConfig.set( MAPRED_TASK_ID, "job_201208090841_013302" );
    mapRunnable.configure( mrJobConfig );

    String actualVariable = mapRunnable.variableSpace.getVariable( INTERNAL_HADOOP_NODE_NUMBER );
    assertEquals( "13302", actualVariable );
  }

  @Test
  public void testMapper_null_output_value() throws Exception {
    transMeta =
      new TransMeta( getClass().getResource( MRTestUtil.PATH_TO_NULL_TEST_TRANSFORMATION ).toURI().getPath() );
    MRTestUtil.configJobMapBaseCase( transMeta, mrJobConfig, mapRunnable );

    reader = new MockRecordReader( Arrays.asList( "test" ) );
    mapRunnable.run( reader, outputCollectorMock, reporterMock );
    Thread.sleep( 300 );
    outputCollectorMock.close();

    assertNull( "Exception thrown", mapRunnable.getException() );
    assertEquals( "Received output when we didn't expect any.  <null>s aren't passed through.", 0,
      outputCollectorMock.getCollection().size() );
  }

  @Test
  public void testMapperNoOutputStep() throws KettleException, URISyntaxException {
    //Turn off displaying stack trace of expected exception to reduce unit test logs
    mrJobConfig.set( "debug", "false" );
    try {
      transMeta = new TransMeta(
        getClass().getResource( MRTestUtil.PATH_TO_NO_OUTPUT_STEP_TEST_TRANSFORMATION ).toURI().getPath() );
      MRTestUtil.configJobMapBaseCase( transMeta, mrJobConfig, mapRunnable );

      reader = new MockRecordReader( Arrays.asList( "test" ) );
      mapRunnable.run( reader, outputCollectorMock, reporterMock );
      fail( "Should have thrown an exception " );
    } catch ( IOException e ) {
      assertTrue( "Test for KettleException", e.getMessage().contains( "Output step not defined in transformation" ) );
    }
  }

  @Test
  public void testMapperBadInjectorFields() throws KettleException, URISyntaxException {
    //Turn off displaying stack trace of expected exception to reduce unit test logs
    mrJobConfig.set( "debug", "false" );
    try {
      transMeta = new TransMeta(
        getClass().getResource( MRTestUtil.PATH_TO_BAD_INJECTOR_STEP_TEST_TRANSFORMATION ).toURI().getPath() );
      MRTestUtil.configJobMapBaseCase( transMeta, mrJobConfig, mapRunnable );

      reader = new MockRecordReader( Arrays.asList( "test" ) );
      mapRunnable.run( reader, outputCollectorMock, reporterMock );
      fail( "Should have thrown an exception" );
    } catch ( IOException e ) {
      assertTrue( "Test for KettleException",
        e.getMessage().contains( "key or value is not defined in transformation injector step" ) );
    }
  }

  @Test
  public void testMapperNoInjectorStep() throws KettleException, URISyntaxException {
    //Turn off displaying stack trace of expected exception to reduce unit test logs
    mrJobConfig.set( "debug", "false" );
    try {
      transMeta = new TransMeta(
        getClass().getResource( MRTestUtil.PATH_TO_NO_INJECTOR_STEP_TEST_TRANSFORMATION ).toURI().getPath() );
      MRTestUtil.configJobMapBaseCase( transMeta, mrJobConfig, mapRunnable );

      reader = new MockRecordReader( Arrays.asList( "test" ) );
      mapRunnable.run( reader, outputCollectorMock, reporterMock );
      fail( "Should have thrown an exception" );
    } catch ( IOException e ) {
      assertTrue( "Test for KettleException",
        e.getMessage().contains( "Unable to find thread with name Injector and copy number 0" ) );
    }
  }

  @Test
  public void testLogChannelLeaking() throws Exception {
    transMeta = new TransMeta(
      getClass().getResource( MRTestUtil.PATH_TO_WORDCOUNT_MAPPER_TEST_TRANSFORMATION ).toURI().getPath() );
    MRTestUtil.configJobMapBaseCase( transMeta, mrJobConfig, mapRunnable );

    int logChannels = LoggingRegistry.getInstance().getMap().size();
    int expectedOutputCollectorMockSize = 0;
    List<IntWritable> expectedWordCountArrays = null;
    assertEquals( "Incorrect output ", expectedOutputCollectorMockSize, outputCollectorMock.getCollection().size() );

    for ( int i = 0; i < RUNS; i++ ) {
      // set up test value rows
      List<String> wordsToCount =
        IntStream.rangeClosed( 1, i + 1 ).mapToObj( value -> String.valueOf( WORD_TO_COUNT_TEMPLATE + value ) )
          .collect( Collectors.toList() );
      reader = new MockRecordReader( wordsToCount );

      mapRunnable.run( reader, outputCollectorMock, reporterMock );

      expectedOutputCollectorMockSize++;
      assertNull( "Exception thrown", mapRunnable.getException() );
      assertEquals( "Incorrect output", expectedOutputCollectorMockSize, outputCollectorMock.getCollection().size() );
      assertEquals( "LogChannels are not being cleaned up. On Run #" + ( i + 1 ) + " we have too many.",
        logChannels + EXPECTED_CHANNELS_PER_RUN, LoggingRegistry.getInstance().getMap().size() );
    }
    outputCollectorMock.close();
    // outputCollectorMock.getCollection().forEach( ( k, v ) -> System.out.println( "outputCollectorMock: Item : " + k +
    // " Count : " + v ) );
    // verifying the arrays of word count for the each word
    for ( int i = RUNS; i > 0; i-- ) {
      expectedWordCountArrays = IntStream.rangeClosed( 1, RUNS - i + 1 ).mapToObj( value -> new IntWritable( 1 ) )
        .collect( Collectors.toList() );
      assertEquals( "Incorrect count array for the word: " + WORD_TO_COUNT_TEMPLATE + i, expectedWordCountArrays,
        outputCollectorMock.getCollection().get( new Text( WORD_TO_COUNT_TEMPLATE + i ) ) );
    }
    assertEquals( logChannels + EXPECTED_CHANNELS_PER_RUN, LoggingRegistry.getInstance().getMap().size() );
  }
}
