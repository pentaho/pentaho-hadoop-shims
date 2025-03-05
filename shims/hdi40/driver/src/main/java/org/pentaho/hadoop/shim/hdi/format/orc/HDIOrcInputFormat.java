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

package org.pentaho.hadoop.shim.hdi.format.orc;

import org.pentaho.hadoop.shim.HadoopShim;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcInputFormat;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HDIOrcInputFormat extends PentahoOrcInputFormat {
  private final org.pentaho.hadoop.shim.api.internal.Configuration pentahoConf;
  private final HadoopShim shim;

  public HDIOrcInputFormat( NamedCluster namedCluster ) {
    super( namedCluster );
    shim = new HadoopShim();
    pentahoConf = shim.createConfiguration( namedCluster );
  }

  @Override
  public IPentahoRecordReader createRecordReader( IPentahoInputSplit split ) {
    requireNonNull( fileName, NOT_NULL_MSG );
    requireNonNull( inputFields, NOT_NULL_MSG );
    return inClassloader( () -> new HDIOrcRecordReader( fileName, conf, inputFields, shim, pentahoConf ) );
  }

  @Override
  public List<IOrcInputField> readSchema() {
    return inClassloader( () -> readSchema(
            HDIOrcRecordReader.getReader(
                    requireNonNull( fileName, NOT_NULL_MSG ), conf, shim, pentahoConf ) ) );
  }
}