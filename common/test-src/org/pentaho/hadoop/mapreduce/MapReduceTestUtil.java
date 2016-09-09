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

import org.apache.hadoop.io.Text;
import org.pentaho.di.trans.TransConfiguration;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransMeta;

/**
 * @author Tatsiana_Kasiankova
 *
 */
public class MapReduceTestUtil {

  protected static final String OUTPUT_STEP = "Output";
  protected static final String INJECTOR_STEP = "Injector";
  protected static final String PATH_TO_NULL_TEST_TRANSFORMATION = "/resources/null-test.ktr";
  protected static final String PATH_TO_NOT_NULL_TEST_TRANSFORMATION = "/resources/not-null-value-test.ktr";
  protected static final String PATH_TO_WORDCOUNT_REDUCER_TEST_TRANSFORMATION = "/resources/wordcount-reducer.ktr";
  protected static final String PATH_TO_WORDCOUNT_MAPPER_TEST_TRANSFORMATION = "/resources/wordcount-mapper.ktr";
  protected static final String PATH_TO_NO_OUTPUT_STEP_TEST_TRANSFORMATION = "/resources/no-output-step.ktr";
  protected static final String PATH_TO_NO_INJECTOR_STEP_TEST_TRANSFORMATION = "/resources/no-injector-step.ktr";
  protected static final String PATH_TO_BAD_INJECTOR_STEP_TEST_TRANSFORMATION = "/resources/bad-injector-fields.ktr";
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

}
