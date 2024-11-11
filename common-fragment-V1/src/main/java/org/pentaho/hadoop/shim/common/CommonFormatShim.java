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

package org.pentaho.hadoop.shim.common;

import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcInputFormat;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.DelegateFormatFactory;
import org.pentaho.hadoop.shim.spi.FormatShim;

public class CommonFormatShim implements FormatShim {

  @Override
  public <T extends IPentahoInputFormat> T  createInputFormat( Class<T> type, NamedCluster namedCluster ) {
    if ( type.isAssignableFrom( IPentahoParquetInputFormat.class ) ) {
      return (T) DelegateFormatFactory.getInputFormatInstance( namedCluster );
    } else if ( type.isAssignableFrom( IPentahoOrcInputFormat.class ) ) {
      return (T) new PentahoOrcInputFormat( namedCluster );
    }
    throw new IllegalArgumentException( "Not supported scheme format" );
  }

  @Override
  public <T extends IPentahoOutputFormat> T createOutputFormat( Class<T> type, NamedCluster namedCluster ) {
    if ( type.isAssignableFrom( IPentahoParquetOutputFormat.class ) ) {
      return (T) DelegateFormatFactory.getOutputFormatInstance( namedCluster );
    } else if ( type.isAssignableFrom( IPentahoOrcOutputFormat.class ) ) {
      return (T) new PentahoOrcOutputFormat( namedCluster );
    }
    throw new IllegalArgumentException( "Not supported scheme format" );
  }

  @Override
  public ShimVersion getVersion() {
    return null;
  }
}
