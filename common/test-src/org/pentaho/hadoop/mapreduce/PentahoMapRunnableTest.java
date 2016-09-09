/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LoggingRegistry;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransMeta;

/**
 * @author Tatsiana_Kasiankova
 *
 */
public class PentahoMapRunnableTest {
  private static final String WORD_TO_COUNT_TEMPLATE = "word";
  private static final String TRANSFORMATION_MAP_OUTPUT_STEPNAME = "transformation-map-output-stepname";
  private static final String TRANSFORMATION_MAP_INPUT_STEPNAME = "transformation-map-input-stepname";
  private static final String TRANSFORMATION_MAP_XML = "transformation-map-xml";
  private static final String INTERNAL_HADOOP_NODE_NUMBER = "Internal.Hadoop.NodeNumber";
  private static final String MAPRED_TASK_ID = "mapred.task.id";
  private static final String MAP_TRANS_META_NAME = "Map transformation";
  /**
   * Mock trans configuration: empty trans meta with name and empty trans execution configuration
   */
  private static final TransConfiguration COMBINER_TRANS_EXEC_CONFIG = MapReduceTestUtil.getTransExecConfig( MapReduceTestUtil.getTransMeta( MAP_TRANS_META_NAME ) );
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
  }

  @Before
  public void setUp() throws KettleException, IOException {
    mapRunnable = new PentahoMapRunnable();
    mrJobConfig = new JobConf();
  }

  @Test
  public void testTaskIdExtraction() throws Exception {
    mrJobConfig.set( TRANSFORMATION_MAP_XML, COMBINER_TRANS_EXEC_CONFIG.getXML() );
    mrJobConfig.set( MAPRED_TASK_ID, "job_201208090841_0133" );
    mapRunnable.configure( mrJobConfig );

    String actualVariable = mapRunnable.variableSpace.getVariable( INTERNAL_HADOOP_NODE_NUMBER );
    assertEquals( "133", actualVariable );
  }

  @Test
  public void testTaskIdExtraction_over_10000() throws Exception {
    mrJobConfig.set( TRANSFORMATION_MAP_XML, COMBINER_TRANS_EXEC_CONFIG.getXML() );
    mrJobConfig.set( MAPRED_TASK_ID, "job_201208090841_013302" );
    mapRunnable.configure( mrJobConfig );

    String actualVariable = mapRunnable.variableSpace.getVariable( INTERNAL_HADOOP_NODE_NUMBER );
    assertEquals( "13302", actualVariable );
  }

  @Test
  public void testMapper_null_output_value() throws Exception {
    transMeta = new TransMeta( getClass().getResource( MapReduceTestUtil.PATH_TO_NULL_TEST_TRANSFORMATION ).toURI().getPath() );
    configJobMapBaseCase( transMeta, mrJobConfig );

    reader = new MockRecordReader( Arrays.asList( "test" ) );
    mapRunnable.run( reader, outputCollectorMock, reporterMock );
    Thread.sleep( 300 );
    outputCollectorMock.close();

    assertNull( "Exception thrown", mapRunnable.getException() );
    assertEquals( "Received output when we didn't expect any.  <null>s aren't passed through.", 0, outputCollectorMock.getCollection().size() );
  }

  @Test
  public void testMapperNoOutputStep() throws IOException, KettleException, URISyntaxException {
    try {
      transMeta = new TransMeta( getClass().getResource( MapReduceTestUtil.PATH_TO_NO_OUTPUT_STEP_TEST_TRANSFORMATION ).toURI().getPath() );
      configJobMapBaseCase( transMeta, mrJobConfig );

      reader = new MockRecordReader( Arrays.asList( "test" ) );
      mapRunnable.run( reader, outputCollectorMock, reporterMock );
      fail( "Should have thrown an exception " );
    } catch ( IOException e ) {
      assertTrue( "Test for KettleException", e.getMessage().contains( "Output step not defined in transformation" ) );
    }
  }

  @Test
  public void testMapperBadInjectorFields() throws IOException, KettleException, URISyntaxException {
    try {
      transMeta = new TransMeta( getClass().getResource( MapReduceTestUtil.PATH_TO_BAD_INJECTOR_STEP_TEST_TRANSFORMATION ).toURI().getPath() );
      configJobMapBaseCase( transMeta, mrJobConfig );

      reader = new MockRecordReader( Arrays.asList( "test" ) );
      mapRunnable.run( reader, outputCollectorMock, reporterMock );
      fail( "Should have thrown an exception" );
    } catch ( IOException e ) {
      assertTrue( "Test for KettleException", e.getMessage().contains( "key or value is not defined in transformation injector step" ) );
    }
  }

  @Test
  public void testMapperNoInjectorStep() throws IOException, KettleException, URISyntaxException {
    try {
      transMeta = new TransMeta( getClass().getResource( MapReduceTestUtil.PATH_TO_NO_INJECTOR_STEP_TEST_TRANSFORMATION ).toURI().getPath() );
      configJobMapBaseCase( transMeta, mrJobConfig );

      reader = new MockRecordReader( Arrays.asList( "test" ) );
      mapRunnable.run( reader, outputCollectorMock, reporterMock );
      fail( "Should have thrown an exception" );
    } catch ( IOException e ) {
      assertTrue( "Test for KettleException", e.getMessage().contains( "Unable to find thread with name Injector and copy number 0" ) );
    }
  }

  @Test
  public void testLogChannelLeaking() throws Exception {
    transMeta = new TransMeta( getClass().getResource( MapReduceTestUtil.PATH_TO_WORDCOUNT_MAPPER_TEST_TRANSFORMATION ).toURI().getPath() );
    configJobMapBaseCase( transMeta, mrJobConfig );

    int logChannels = LoggingRegistry.getInstance().getMap().size();
    int expectedOutputCollectorMockSize = 0;
    List<IntWritable> expectedWordCountArrays = null;
    IntStream.rangeClosed( 1, RUNS ).mapToObj( value -> new IntWritable( 1 ) ).collect( Collectors.toList() );
    assertEquals( "Incorrect output ", expectedOutputCollectorMockSize, outputCollectorMock.getCollection().size() );

    for ( int i = 0; i < RUNS; i++ ) {
      // set up test value rows
      List<String> wordsToCount = IntStream.rangeClosed( 1, i + 1 ).mapToObj( value -> String.valueOf( WORD_TO_COUNT_TEMPLATE + value ) ).collect( Collectors.toList() );

      // wordsToCount.forEach( System.out::println );
      reader = new MockRecordReader( wordsToCount );

      mapRunnable.run( reader, outputCollectorMock, reporterMock );

      expectedOutputCollectorMockSize++;
      assertNull( "Exception thrown", mapRunnable.getException() );
      assertEquals( "Incorrect output", expectedOutputCollectorMockSize, outputCollectorMock.getCollection().size() );
      assertEquals( "LogChannels are not being cleaned up. On Run #" + ( i + 1 ) + " we have too many.", logChannels + EXPECTED_CHANNELS_PER_RUN, LoggingRegistry.getInstance().getMap().size() );
    }
    outputCollectorMock.close();
    // outputCollectorMock.getCollection().forEach( ( k, v ) -> System.out.println( "outputCollectorMock: Item : " + k +
    // " Count : " + v ) );
    // verifying the arrays of word count for the each word
    for ( int i = RUNS; i > 0; i-- ) {
      System.out.println( "Iteration for the word: " + WORD_TO_COUNT_TEMPLATE + i );
      expectedWordCountArrays = IntStream.rangeClosed( 1, RUNS - i + 1 ).mapToObj( value -> new IntWritable( 1 ) ).collect( Collectors.toList() );
      assertEquals( "Incorrect count array for the word: " + WORD_TO_COUNT_TEMPLATE + i, expectedWordCountArrays, outputCollectorMock.getCollection().get( new Text( WORD_TO_COUNT_TEMPLATE + i ) ) );
    }
    assertEquals( logChannels + EXPECTED_CHANNELS_PER_RUN, LoggingRegistry.getInstance().getMap().size() );
  }

  /**
   * Set up properties for base job map transformation.
   *
   * @param transMeta
   * @throws IOException
   * @throws KettleException
   */
  private void configJobMapBaseCase( TransMeta transMeta, JobConf mrJobConfig ) throws IOException, KettleException {
    // mrJobConfig.set( "debug", "true" );
    mrJobConfig.set( TRANSFORMATION_MAP_XML, MapReduceTestUtil.getTransExecConfig( transMeta ).getXML() );
    mrJobConfig.set( TRANSFORMATION_MAP_INPUT_STEPNAME, MapReduceTestUtil.INJECTOR_STEP );
    mrJobConfig.set( TRANSFORMATION_MAP_OUTPUT_STEPNAME, MapReduceTestUtil.OUTPUT_STEP );
    mrJobConfig.setOutputKeyClass( Text.class );
    mrJobConfig.setOutputValueClass( IntWritable.class );
    mapRunnable.configure( mrJobConfig );
  }
}
