/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.trans.TransMeta;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
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
public class PentahoMapReduceTest {

  private static LogChannelInterface log = new LogChannel( PentahoMapReduceTest.class.getName() );

  private static final String WORDS_TO_CALCULATE = "zebra giraffe hippo elephant tiger";
  private Reporter reporterMock = mock( Reporter.class );
  private PentahoMapRunnable mapRunnable;
  private JobConf mrJobConfig;
  private TransMeta transMeta;
  private MockOutputCollector outputCollectorMock = new MockOutputCollector();
  private MockOutputCollector reducerInputCollectorMock = outputCollectorMock;
  private MockRecordReader reader;
  private GenericTransReduce genericTransReduce;
  private static final int ROWS_TO_CALCULATE = 10000;

  @BeforeClass
  public static void before() throws KettleException {
    KettleEnvironment.init();
  }

  @Before
  public void setUp() throws KettleException, IOException {
    mapRunnable = new PentahoMapRunnable();
    genericTransReduce = new GenericTransReduce();
    mrJobConfig = new JobConf();
    //Turn off all debug messages from PentahoMapRunnable to reduce unit test logs.Turn it on if it needs for debug.
    mrJobConfig.set( "logLevel", LogLevel.ERROR.name() );
  }

  @Test
  public void testMapRunnable_WordCount() throws IOException, KettleException, URISyntaxException {
    // Create mapper transformations and configure job with the appropriate settings
    transMeta = new TransMeta(
      getClass().getResource( MRTestUtil.PATH_TO_WORDCOUNT_MAPPER_TEST_TRANSFORMATION ).toURI().getPath() );
    MRTestUtil.configJobMapBaseCase( transMeta, mrJobConfig, mapRunnable );
    // Create reducer transformations and configure job with the appropriate settings
    transMeta = new TransMeta(
      getClass().getResource( MRTestUtil.PATH_TO_WORDCOUNT_REDUCER_TEST_TRANSFORMATION ).toURI().getPath() );
    MRTestUtil.configJobReducerBaseCase( transMeta, mrJobConfig, genericTransReduce );

    // Create data rows with words to count
    List<String> wordsToCalculate =
      IntStream.rangeClosed( 1, ROWS_TO_CALCULATE ).mapToObj( value -> String.valueOf( WORDS_TO_CALCULATE ) )
        .collect( Collectors.toList() );
    if ( log.isDebug() ) {
      log.logDebug( "Mapper input data: " + ROWS_TO_CALCULATE + " rows of [" + WORDS_TO_CALCULATE + "]" );
    }
    reader = new MockRecordReader( wordsToCalculate );
    // execute mapper
    long start = System.currentTimeMillis();
    mapRunnable.run( reader, outputCollectorMock, reporterMock );
    outputCollectorMock.close();
    long stop = System.currentTimeMillis();

    if ( log.isDebug() ) {
      log.logDebug( "Executed " + ROWS_TO_CALCULATE + " in " + ( stop - start ) + "ms" );
      log.logDebug( "Average: " + ( ( stop - start ) / (float) ROWS_TO_CALCULATE ) + "ms" );
      log.logDebug( "Rows/Second: " + ( ROWS_TO_CALCULATE / ( ( stop - start ) / 1000f ) ) );
    }

    if ( log.isDebug() ) {
      outputCollectorMock.getCollection()
        .forEach( ( k, v ) -> log.logDebug( "Mapper output data: " + k + "=" + v ) );
    }
    assertNull( "Exception thrown", mapRunnable.getException() );
    assertNotNull( outputCollectorMock );
    assertNotNull( outputCollectorMock.getCollection() );
    assertNotNull( outputCollectorMock.getCollection().keySet() );
    assertEquals( 5, outputCollectorMock.getCollection().keySet().size() );

    // verifying the arrays of word count for the each word
    List<IntWritable> expectedWordCountArrays =
      IntStream.rangeClosed( 1, ROWS_TO_CALCULATE ).mapToObj( value -> new IntWritable( 1 ) )
        .collect( Collectors.toList() );
    for ( Object key : outputCollectorMock.getCollection().keySet() ) {
      assertEquals( "Incorrect count array for the word: " + key, expectedWordCountArrays,
        outputCollectorMock.getCollection().get( new Text( key.toString() ) ) );
    }

    // input data for reducer is going to be taken from mapper output data
    reducerInputCollectorMock = outputCollectorMock;
    if ( log.isDebug() ) {
      reducerInputCollectorMock.getCollection()
        .forEach( ( k, v ) -> log.logDebug( "Reducer input data: " + k + "=" + v ) );
    }
    outputCollectorMock = new MockOutputCollector();

    // execute reducer
    start = System.currentTimeMillis();
    for ( Object key : reducerInputCollectorMock.getCollection().keySet() ) {
      genericTransReduce
        .reduce( (Text) key, new ArrayList( reducerInputCollectorMock.getCollection().get( key ) ).iterator(),
          outputCollectorMock, reporterMock );
      genericTransReduce.close();
    }
    outputCollectorMock.close();
    stop = System.currentTimeMillis();

    if ( log.isDebug() ) {
      outputCollectorMock.getCollection()
        .forEach( ( k, v ) -> log.logDebug( "Reducer output data: " + k + "=" + v ) );
    }
    // verifying reduced data
    assertNull( "Exception thrown", genericTransReduce.getException() );
    assertNotNull( outputCollectorMock );
    assertNotNull( outputCollectorMock.getCollection() );
    assertNotNull( outputCollectorMock.getCollection().keySet() );
    assertEquals( 5, outputCollectorMock.getCollection().keySet().size() );

    IntWritable expectedWordCount =
      new IntWritable( expectedWordCountArrays.stream().mapToInt( IntWritable::get ).sum() );
    for ( String wordToCount : Arrays.asList( WORDS_TO_CALCULATE.split( " " ) ) ) {
      assertEquals( expectedWordCount, outputCollectorMock.getCollection().get( new Text( wordToCount ) ).get( 0 ) );
    }
  }

}
