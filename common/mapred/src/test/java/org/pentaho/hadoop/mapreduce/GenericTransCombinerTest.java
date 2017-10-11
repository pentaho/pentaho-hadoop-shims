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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

/**
 * @author Tatsiana_Kasiankova
 */
@SuppressWarnings( { "unchecked", "rawtypes" } )
public class GenericTransCombinerTest {
  private static final String COMBINER_OUTPUT_STEPNAME = "combiner-output-stepname";
  private static final String COMBINER_INPUT_STEPNAME = "combiner-input-stepname";
  private static final String COMBINER_TRANS_META_NAME = "Combiner transformation";
  private static TransConfiguration combinerTransExecConfig;
  /**
   * We expect 4 log channels per run. The total should never grow past logChannelsBefore + 4.
   */
  final int EXPECTED_CHANNELS_PER_RUN = 4;
  /**
   * Run the reducer this many times
   */
  final int RUNS = 10;
  private Reporter reporterMock = mock( Reporter.class );

  private GenericTransCombiner genericTransCombiner;
  private JobConf mrJobConfig;
  private TransMeta transMeta;
  private MockOutputCollector outputCollectorMock = new MockOutputCollector();

  @BeforeClass
  public static void before() throws KettleException {
    KettleEnvironment.init();
    combinerTransExecConfig = MRTestUtil.getTransExecConfig( MRTestUtil.getTransMeta( COMBINER_TRANS_META_NAME ) );
  }

  @Before
  public void setUp() throws KettleException, IOException {
    genericTransCombiner = new GenericTransCombiner();
    mrJobConfig = new JobConf();
    //Turn off all debug messages from PentahoMapRunnable to reduce unit test logs
    mrJobConfig.set( "logLevel", LogLevel.ERROR.name() );
  }

  @Test
  public void testCombinerOutputClasses() throws IOException, KettleException {
    mrJobConfig.set( MRTestUtil.TRANSFORMATION_COMBINER_XML, combinerTransExecConfig.getXML() );
    mrJobConfig.setMapOutputKeyClass( Text.class );
    mrJobConfig.setMapOutputValueClass( IntWritable.class );

    genericTransCombiner.configure( mrJobConfig );

    assertEquals( mrJobConfig.getMapOutputKeyClass(), genericTransCombiner.getOutClassK() );
    assertEquals( mrJobConfig.getMapOutputValueClass(), genericTransCombiner.getOutClassV() );
  }

  @Test
  public void testCombinerInputOutputSteps() throws IOException, KettleException {
    mrJobConfig.set( MRTestUtil.TRANSFORMATION_COMBINER_XML, combinerTransExecConfig.getXML() );
    mrJobConfig.set( MRTestUtil.TRANSFORMATION_COMBINER_INPUT_STEPNAME, COMBINER_INPUT_STEPNAME );
    mrJobConfig.set( MRTestUtil.TRANSFORMATION_COMBINER_OUTPUT_STEPNAME, COMBINER_OUTPUT_STEPNAME );

    assertNull( genericTransCombiner.getInputStepName() );
    assertNull( genericTransCombiner.getOutputStepName() );

    genericTransCombiner.configure( mrJobConfig );

    assertEquals( COMBINER_INPUT_STEPNAME, genericTransCombiner.getInputStepName() );
    assertEquals( COMBINER_OUTPUT_STEPNAME, genericTransCombiner.getOutputStepName() );
  }

  @Test
  public void testCombiner_null_output_value() throws Exception {
    transMeta =
      new TransMeta( getClass().getResource( MRTestUtil.PATH_TO_NULL_TEST_TRANSFORMATION ).toURI().getPath() );
    MRTestUtil.configJobCombinerBaseCase( transMeta, mrJobConfig, genericTransCombiner );

    genericTransCombiner
      .reduce( MRTestUtil.KEY_TO_NULL, Arrays.asList( MRTestUtil.VALUE_TO_NULL ).iterator(), outputCollectorMock,
        reporterMock );
    genericTransCombiner.close();
    outputCollectorMock.close();

    assertNull( "Exception thrown", genericTransCombiner.getException() );
    assertEquals( "Received output when we didn't expect any.  <null>s aren't passed through.", 0,
      outputCollectorMock.getCollection().size() );
  }

  @Test
  public void testCombiner_not_null_outputValues() throws Exception {
    transMeta =
      new TransMeta( getClass().getResource( MRTestUtil.PATH_TO_NOT_NULL_TEST_TRANSFORMATION ).toURI().getPath() );
    MRTestUtil.configJobCombinerBaseCase( transMeta, mrJobConfig, genericTransCombiner );

    Text expectedKey = new Text( "test" );
    List<IntWritable> expectedValue = Arrays.asList( new IntWritable( 8 ), new IntWritable( 9 ) );

    genericTransCombiner.reduce( expectedKey, expectedValue.iterator(), outputCollectorMock, reporterMock );
    genericTransCombiner.close();
    outputCollectorMock.close();

    assertNull( "Exception thrown", genericTransCombiner.getException() );
    assertEquals( 1, outputCollectorMock.getCollection().size() );
    assertEquals( expectedValue, outputCollectorMock.getCollection().get( expectedKey ) );
  }

  @Test
  public void testLogChannelLeaking() throws Exception {
    transMeta = new TransMeta(
      getClass().getResource( MRTestUtil.PATH_TO_WORDCOUNT_REDUCER_TEST_TRANSFORMATION ).toURI().getPath() );
    MRTestUtil.configJobCombinerBaseCase( transMeta, mrJobConfig, genericTransCombiner );

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

      genericTransCombiner.reduce( wordToCount, wordCounts.iterator(), outputCollectorMock, reporterMock );
      genericTransCombiner.close();

      expectedOutputCollectorMockSize++;
      assertNull( "Exception thrown", genericTransCombiner.getException() );
      assertEquals( "Incorrect output", expectedOutputCollectorMockSize, outputCollectorMock.getCollection().size() );
      assertEquals( expectedWordCount, outputCollectorMock.getCollection().get( wordToCount ).get( 0 ) );
      assertEquals( "LogChannels are not being cleaned up. On Run #" + ( i + 1 ) + " we have too many.",
        logChannels + EXPECTED_CHANNELS_PER_RUN, LoggingRegistry.getInstance().getMap().size() );
    }
    outputCollectorMock.close();
    assertEquals( logChannels + EXPECTED_CHANNELS_PER_RUN, LoggingRegistry.getInstance().getMap().size() );
  }
}
