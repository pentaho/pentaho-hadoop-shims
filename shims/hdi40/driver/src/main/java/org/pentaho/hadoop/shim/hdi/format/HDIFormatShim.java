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
package org.pentaho.hadoop.shim.hdi.format;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.common.CommonFormatShim;
import org.pentaho.hadoop.shim.hdi.format.orc.HDIOrcInputFormat;
import org.pentaho.hadoop.shim.hdi.format.orc.HDIOrcOutputFormat;
import org.pentaho.hadoop.shim.hdi.format.parquet.HDIApacheInputFormat;
import org.pentaho.hadoop.shim.hdi.format.parquet.HDIApacheOutputFormat;

public class HDIFormatShim extends CommonFormatShim {
  @Override
  public <T extends IPentahoInputFormat> T createInputFormat( Class<T> type, NamedCluster namedCluster ) {
    if ( type.isAssignableFrom( IPentahoParquetInputFormat.class ) ) {
      return (T) new HDIApacheInputFormat( namedCluster );
    } else if ( type.isAssignableFrom( IPentahoOrcInputFormat.class ) ) {
      return (T) new HDIOrcInputFormat( namedCluster );
    }
    throw new IllegalArgumentException( "Not supported HDI scheme format" );
  }

  @Override
  public <T extends IPentahoOutputFormat> T createOutputFormat( Class<T> type, NamedCluster namedCluster ) {
    if ( type.isAssignableFrom( IPentahoParquetOutputFormat.class ) ) {
      return (T) new HDIApacheOutputFormat( namedCluster );
    } else if ( type.isAssignableFrom( IPentahoOrcOutputFormat.class ) ) {
      return (T) new HDIOrcOutputFormat( namedCluster );
    }
    throw new IllegalArgumentException( "Not supported HDI scheme format" );
  }
}