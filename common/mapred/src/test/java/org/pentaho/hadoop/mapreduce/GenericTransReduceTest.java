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
import org.apache.hadoop.io.LongWritable;
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
 * User: Dzmitry Stsiapanau Date: 10/29/14 Time: 12:44 PM
 */
@SuppressWarnings( { "unchecked", "rawtypes" } )
public class GenericTransReduceTest {
  private static final String REDUCE_OUTPUT_STEPNAME = "reduce-output-stepname";
  private static final String REDUCE_INPUT_STEPNAME = "reduce-input-stepname";
  private static final String REDUCER_TRANS_META_NAME = "Reducer transformation";
  /**
   * We expect 4 log channels per run. The total should never grow past logChannelsBefore + 4.
   */
  final int EXPECTED_CHANNELS_PER_RUN = 4;
  /**
   * Run the reducer this many times
   */
  final int RUNS = 10;
  private static TransConfiguration reducerTransExecConfig;

  private Reporter reporterMock = mock( Reporter.class );

  private GenericTransReduce genericTransReduce;
  private JobConf mrJobConfig;
  private TransMeta transMeta;
  private MockOutputCollector outputCollectorMock = new MockOutputCollector();

  @BeforeClass
  public static void before() throws KettleException {
    KettleEnvironment.init();
    reducerTransExecConfig = MRTestUtil.getTransExecConfig( MRTestUtil.getTransMeta( REDUCER_TRANS_META_NAME ) );
  }

  @Before
  public void setUp() throws KettleException, IOException {
    genericTransReduce = new GenericTransReduce();
    mrJobConfig = new JobConf();
    // Turn off all debug messages from PentahoMapRunnable to reduce unit test logs
    mrJobConfig.set( "logLevel", LogLevel.ERROR.name() );
  }

  @Test
  public void testClose() throws KettleException, IOException {
    GenericTransReduce gtr = new GenericTransReduce();
    try {
      gtr.close();
    } catch ( NullPointerException ex ) {
      ex.printStackTrace();
      fail( " Null pointer on close look PDI-13080 " + ex.getMessage() );
    }
  }

  @Test
  public void testReducerOutputClasses() throws IOException, KettleException {
    mrJobConfig.set( MRTestUtil.TRANSFORMATION_REDUCE_XML, reducerTransExecConfig.getXML() );

    mrJobConfig.setOutputKeyClass( Text.class );
    mrJobConfig.setOutputValueClass( LongWritable.class );

    genericTransReduce.configure( mrJobConfig );

    assertEquals( mrJobConfig.getOutputKeyClass(), genericTransReduce.getOutClassK() );
    assertEquals( mrJobConfig.getOutputValueClass(), genericTransReduce.getOutClassV() );
  }

  @Test
  public void testReducerInputOutputSteps() throws IOException, KettleException {
    mrJobConfig.set( MRTestUtil.TRANSFORMATION_REDUCE_XML, reducerTransExecConfig.getXML() );
    mrJobConfig.set( MRTestUtil.TRANSFORMATION_REDUCE_INPUT_STEPNAME, REDUCE_INPUT_STEPNAME );
    mrJobConfig.set( MRTestUtil.TRANSFORMATION_REDUCE_OUTPUT_STEPNAME, REDUCE_OUTPUT_STEPNAME );

    assertNull( genericTransReduce.getInputStepName() );
    assertNull( genericTransReduce.getOutputStepName() );

    genericTransReduce.configure( mrJobConfig );

    assertEquals( REDUCE_INPUT_STEPNAME, genericTransReduce.getInputStepName() );
    assertEquals( REDUCE_OUTPUT_STEPNAME, genericTransReduce.getOutputStepName() );
  }

  @Test
  public void testReducer_null_output_value() throws Exception {
    transMeta =
      new TransMeta( getClass().getResource( MRTestUtil.PATH_TO_NULL_TEST_TRANSFORMATION ).toURI().getPath() );
    MRTestUtil.configJobReducerBaseCase( transMeta, mrJobConfig, genericTransReduce );

    genericTransReduce
      .reduce( MRTestUtil.KEY_TO_NULL, Arrays.asList( MRTestUtil.VALUE_TO_NULL ).iterator(), outputCollectorMock,
        reporterMock );
    genericTransReduce.close();
    outputCollectorMock.close();

    Exception ex = genericTransReduce.getException();
    assertNull( "Exception thrown", ex );
    assertEquals( "Received output when we didn't expect any.  <null>s aren't passed through.", 0,
      outputCollectorMock.getCollection().size() );
  }

  @Test
  public void testReducer_not_null_outputValues() throws Exception {
    transMeta =
      new TransMeta( getClass().getResource( MRTestUtil.PATH_TO_NOT_NULL_TEST_TRANSFORMATION ).toURI().getPath() );
    MRTestUtil.configJobReducerBaseCase( transMeta, mrJobConfig, genericTransReduce );

    Text expectedKey = new Text( "test" );
    List<IntWritable> expectedValue = Arrays.asList( new IntWritable( 8 ), new IntWritable( 9 ) );

    genericTransReduce.reduce( expectedKey, expectedValue.iterator(), outputCollectorMock, reporterMock );
    genericTransReduce.close();
    outputCollectorMock.close();

    Exception ex = genericTransReduce.getException();
    assertNull( "Exception thrown", ex );
    assertEquals( 1, outputCollectorMock.getCollection().size() );
    assertEquals( expectedValue, outputCollectorMock.getCollection().get( expectedKey ) );
  }

  @Test
  public void testReducer_WordCount() throws Exception {
    transMeta = new TransMeta(
      getClass().getResource( MRTestUtil.PATH_TO_WORDCOUNT_REDUCER_TEST_TRANSFORMATION ).toURI().getPath() );
    MRTestUtil.configJobReducerBaseCase( transMeta, mrJobConfig, genericTransReduce );

    Text wordToCount = new Text( "test" );
    IntWritable[] wordCountArray =
      new IntWritable[] { new IntWritable( 8 ), new IntWritable( 9 ), new IntWritable( 1 ) };
    IntWritable expectedWordCount =
      new IntWritable( Arrays.stream( wordCountArray ).mapToInt( IntWritable::get ).sum() );

    genericTransReduce
      .reduce( wordToCount, Arrays.asList( wordCountArray ).iterator(), outputCollectorMock, reporterMock );
    genericTransReduce.close();
    outputCollectorMock.close();

    Exception ex = genericTransReduce.getException();
    assertNull( "Exception thrown", ex );
    assertEquals( 1, outputCollectorMock.getCollection().size() );
    assertEquals( expectedWordCount, outputCollectorMock.getCollection().get( wordToCount ).get( 0 ) );
  }

  @Test
  public void testLogChannelLeaking() throws Exception {
    transMeta = new TransMeta(
      getClass().getResource( MRTestUtil.PATH_TO_WORDCOUNT_REDUCER_TEST_TRANSFORMATION ).toURI().getPath() );
    MRTestUtil.configJobReducerBaseCase( transMeta, mrJobConfig, genericTransReduce );

    int logChannels = LoggingRegistry.getInstance().getMap().size();
    Text wordToCount = null;
    int expectedOutputCollectorMockSize = 0;
    assertEquals( "Incorrect output", expectedOutputCollectorMockSize, outputCollectorMock.getCollection().size() );

    for ( int i = 0; i < RUNS; i++ ) {
      // set up test key and value for reducer as a pair of elements: word1-->[1], word2-->[1,2] ...,
      // wordN-->[1,...,N-1,N]
      wordToCount = new Text( "word" + ( i + 1 ) );
      List<IntWritable> wordCounts =
        IntStream.rangeClosed( 1, i + 1 ).mapToObj( value -> new IntWritable( value ) ).collect( Collectors.toList() );
      IntWritable expectedWordCount = new IntWritable( wordCounts.stream().mapToInt( IntWritable::get ).sum() );

      genericTransReduce.reduce( wordToCount, wordCounts.iterator(), outputCollectorMock, reporterMock );
      genericTransReduce.close();

      expectedOutputCollectorMockSize++;
      assertNull( "Exception thrown", genericTransReduce.getException() );
      assertEquals( "Incorrect output", expectedOutputCollectorMockSize, outputCollectorMock.getCollection().size() );
      assertEquals( expectedWordCount, outputCollectorMock.getCollection().get( wordToCount ).get( 0 ) );
      assertEquals( "LogChannels are not being cleaned up. On Run #" + ( i + 1 ) + " we have too many.",
        logChannels + EXPECTED_CHANNELS_PER_RUN, LoggingRegistry.getInstance().getMap().size() );
    }
    outputCollectorMock.close();
    assertEquals( logChannels + EXPECTED_CHANNELS_PER_RUN, LoggingRegistry.getInstance().getMap().size() );
  }

  @Test
  public void testReducerNoOutputStep() throws KettleException, URISyntaxException {
    //Turn off displaying stack trace of expected exception to reduce unit test logs
    mrJobConfig.set( "debug", "false" );
    try {
      transMeta = new TransMeta(
        getClass().getResource( MRTestUtil.PATH_TO_NO_OUTPUT_STEP_TEST_TRANSFORMATION ).toURI().getPath() );
      MRTestUtil.configJobReducerBaseCase( transMeta, mrJobConfig, genericTransReduce );

      genericTransReduce
        .reduce( new Text( "key" ), Arrays.asList( new IntWritable( 8 ) ).iterator(), outputCollectorMock,
          reporterMock );
      genericTransReduce.close();
      fail( "Should have thrown an exception " );
    } catch ( IOException e ) {
      assertTrue( "Test for KettleException", e.getMessage().contains( "Output step not defined in transformation" ) );
    }
  }

  @Test
  public void testReducerBadInjectorFields() throws KettleException, URISyntaxException {
    //Turn off displaying stack trace of expected exception to reduce unit test logs
    mrJobConfig.set( "debug", "false" );
    try {
      transMeta = new TransMeta(
        getClass().getResource( MRTestUtil.PATH_TO_BAD_INJECTOR_STEP_TEST_TRANSFORMATION ).toURI().getPath() );
      MRTestUtil.configJobReducerBaseCase( transMeta, mrJobConfig, genericTransReduce );

      genericTransReduce
        .reduce( new Text( "key" ), Arrays.asList( new IntWritable( 8 ) ).iterator(), outputCollectorMock,
          reporterMock );
      fail( "Should have thrown an exception" );
    } catch ( IOException e ) {
      assertTrue( "Test for KettleException",
        e.getMessage().contains( "key or value is not defined in transformation injector step" ) );
    }
  }

  @Test
  public void testReducerNoInjectorStep() throws IOException, KettleException, URISyntaxException {
    //Turn off displaying stack trace of expected exception to reduce unit test logs
    mrJobConfig.set( "debug", "false" );
    try {
      transMeta = new TransMeta(
        getClass().getResource( MRTestUtil.PATH_TO_NO_INJECTOR_STEP_TEST_TRANSFORMATION ).toURI().getPath() );
      MRTestUtil.configJobReducerBaseCase( transMeta, mrJobConfig, genericTransReduce );

      genericTransReduce
        .reduce( new Text( "key" ), Arrays.asList( new IntWritable( 8 ) ).iterator(), outputCollectorMock,
          reporterMock );
      fail( "Should have thrown an exception" );
    } catch ( IOException e ) {
      assertTrue( "Test for KettleException",
        e.getMessage().contains( "Unable to find thread with name Injector and copy number 0" ) );
    }
  }
}
