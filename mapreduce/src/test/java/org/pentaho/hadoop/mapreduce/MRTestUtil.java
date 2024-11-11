/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;

import java.io.IOException;

/**
 * @author Tatsiana_Kasiankova
 */
@SuppressWarnings( { "rawtypes" } )
public class MRTestUtil {

  protected static final String OUTPUT_STEP = "Output";
  protected static final String INJECTOR_STEP = "Injector";
  protected static final String TRANSFORMATION_REDUCE_OUTPUT_STEPNAME = "transformation-reduce-output-stepname";
  protected static final String TRANSFORMATION_REDUCE_INPUT_STEPNAME = "transformation-reduce-input-stepname";
  protected static final String TRANSFORMATION_REDUCE_XML = "transformation-reduce-xml";
  protected static final String TRANSFORMATION_COMBINER_OUTPUT_STEPNAME = "transformation-combiner-output-stepname";
  protected static final String TRANSFORMATION_COMBINER_INPUT_STEPNAME = "transformation-combiner-input-stepname";
  protected static final String TRANSFORMATION_COMBINER_XML = "transformation-combiner-xml";
  protected static final String TRANSFORMATION_MAP_OUTPUT_STEPNAME = "transformation-map-output-stepname";
  protected static final String TRANSFORMATION_MAP_INPUT_STEPNAME = "transformation-map-input-stepname";
  protected static final String TRANSFORMATION_MAP_XML = "transformation-map-xml";

  protected static final String PATH_TO_NULL_TEST_TRANSFORMATION = "/null-test.ktr";
  protected static final String PATH_TO_NOT_NULL_TEST_TRANSFORMATION = "/not-null-value-test.ktr";
  protected static final String PATH_TO_WORDCOUNT_REDUCER_TEST_TRANSFORMATION = "/wordcount-reducer.ktr";
  protected static final String PATH_TO_WORDCOUNT_MAPPER_TEST_TRANSFORMATION = "/wordcount-mapper.ktr";
  protected static final String PATH_TO_NO_OUTPUT_STEP_TEST_TRANSFORMATION = "/no-output-step.ktr";
  protected static final String PATH_TO_NO_INJECTOR_STEP_TEST_TRANSFORMATION = "/no-injector-step.ktr";
  protected static final String PATH_TO_BAD_INJECTOR_STEP_TEST_TRANSFORMATION = "/bad-injector-fields.ktr";
  protected static final String PATH_TO_MR_PASSTHROUGH_TEST_TRANSFORMATION = "/resources/mr-passthrough.ktr";
  protected static final Text KEY_TO_NULL = new Text( "0" );
  protected static final Text VALUE_TO_NULL = new Text( "test" );

  protected static TransMeta getTransMeta( String transMetaNname ) {
    TransMeta transMeta = new TransMeta();
    transMeta.setName( transMetaNname );
    return transMeta;
  }

  protected static TransConfiguration getTransExecConfig( TransMeta trMeta ) {
    TransExecutionConfiguration transExecConfig = new TransExecutionConfiguration();
    return new TransConfiguration( trMeta, transExecConfig );
  }

  /**
   * Set up properties for base job reducer transformation.
   *
   * @param transMeta
   * @param genericTransReduce
   * @throws IOException
   * @throws KettleException
   */
  protected static void configJobReducerBaseCase( TransMeta transMeta, JobConf mrJobConfig,
                                                  GenericTransReduce genericTransReduce )
    throws IOException, KettleException {
    // mrJobConfig.set( "debug", "true" );
    mrJobConfig.set( TRANSFORMATION_REDUCE_XML, MRTestUtil.getTransExecConfig( transMeta ).getXML() );
    mrJobConfig.set( TRANSFORMATION_REDUCE_INPUT_STEPNAME, MRTestUtil.INJECTOR_STEP );
    mrJobConfig.set( TRANSFORMATION_REDUCE_OUTPUT_STEPNAME, MRTestUtil.OUTPUT_STEP );
    mrJobConfig.setOutputKeyClass( Text.class );
    mrJobConfig.setOutputValueClass( IntWritable.class );
    genericTransReduce.configure( mrJobConfig );
  }

  /**
   * Set up properties for base job combiner transformation.
   *
   * @param transMeta
   * @throws IOException
   * @throws KettleException
   */
  protected static void configJobCombinerBaseCase( TransMeta transMeta, JobConf mrJobConfig,
                                                   GenericTransCombiner genericTransCombiner )
    throws IOException, KettleException {
    // mrJobConfig.set( "debug", "true" );
    mrJobConfig.set( TRANSFORMATION_COMBINER_XML, MRTestUtil.getTransExecConfig( transMeta ).getXML() );
    mrJobConfig.set( TRANSFORMATION_COMBINER_INPUT_STEPNAME, MRTestUtil.INJECTOR_STEP );
    mrJobConfig.set( TRANSFORMATION_COMBINER_OUTPUT_STEPNAME, MRTestUtil.OUTPUT_STEP );
    mrJobConfig.setOutputKeyClass( Text.class );
    mrJobConfig.setOutputValueClass( IntWritable.class );
    genericTransCombiner.configure( mrJobConfig );
  }

  /**
   * Set up properties for base job map transformation.
   *
   * @param transMeta
   * @throws IOException
   * @throws KettleException
   */
  static void configJobMapBaseCase( TransMeta transMeta, JobConf mrJobConfig, PentahoMapRunnable mapRunnable )
    throws IOException, KettleException {
    // mrJobConfig.set( "debug", "true" );
    mrJobConfig.set( TRANSFORMATION_MAP_XML, MRTestUtil.getTransExecConfig( transMeta ).getXML() );
    mrJobConfig.set( TRANSFORMATION_MAP_INPUT_STEPNAME, MRTestUtil.INJECTOR_STEP );
    mrJobConfig.set( TRANSFORMATION_MAP_OUTPUT_STEPNAME, MRTestUtil.OUTPUT_STEP );
    mrJobConfig.setOutputKeyClass( Text.class );
    mrJobConfig.setOutputValueClass( IntWritable.class );
    mapRunnable.configure( mrJobConfig );
  }

}
