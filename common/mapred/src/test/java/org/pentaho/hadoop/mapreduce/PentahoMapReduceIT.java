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
public class PentahoMapReduceIT {
  //Turn off debug messages for the tests.
  private static final boolean DEBUG_MODE = false;
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
    if ( DEBUG_MODE ) {
      System.out.println( "Mapper input data: " + ROWS_TO_CALCULATE + " rows of [" + WORDS_TO_CALCULATE + "]" );
    }
    reader = new MockRecordReader( wordsToCalculate );
    // execute mapper
    long start = System.currentTimeMillis();
    mapRunnable.run( reader, outputCollectorMock, reporterMock );
    outputCollectorMock.close();
    long stop = System.currentTimeMillis();

    if ( DEBUG_MODE ) {
      System.out.println( "Executed " + ROWS_TO_CALCULATE + " in " + ( stop - start ) + "ms" );
      System.out.println( "Average: " + ( ( stop - start ) / (float) ROWS_TO_CALCULATE ) + "ms" );
      System.out.println( "Rows/Second: " + ( ROWS_TO_CALCULATE / ( ( stop - start ) / 1000f ) ) );
    }

    if ( DEBUG_MODE ) {
      outputCollectorMock.getCollection()
        .forEach( ( k, v ) -> System.out.println( "Mapper output data: " + k + "=" + v ) );
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
    if ( DEBUG_MODE ) {
      reducerInputCollectorMock.getCollection()
        .forEach( ( k, v ) -> System.out.println( "Reducer input data: " + k + "=" + v ) );
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

    if ( DEBUG_MODE ) {
      outputCollectorMock.getCollection()
        .forEach( ( k, v ) -> System.out.println( "Reducer output data: " + k + "=" + v ) );
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
