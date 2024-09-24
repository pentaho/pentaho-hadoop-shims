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

package org.pentaho.hadoop.shim.api.mapreduce;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.TransMeta;

/**
 * Created by bryan on 1/7/16.
 */
public interface PentahoMapReduceJobBuilder extends MapReduceJobBuilder {
  String getHadoopWritableCompatibleClassName( ValueMetaInterface valueMetaInterface );

  void setMapperInfo( String mapperTransformationXml, String mapperInputStep, String mapperOutputStep );

  void setCombinerInfo( String combinerTransformationXml, String combinerInputStep, String combinerOutputStep );

  void setReducerInfo( String reducerTransformationXml, String reducerInputStep, String reducerOutputStep );

  void setLogLevel( LogLevel logLevel );

  void setCleanOutputPath( boolean cleanOutputPath );

  void verifyTransMeta( TransMeta transMeta, String inputStepName, String outputStepName ) throws KettleException;
}
