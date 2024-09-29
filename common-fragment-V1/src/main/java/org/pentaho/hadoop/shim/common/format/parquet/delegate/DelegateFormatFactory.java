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


package org.pentaho.hadoop.shim.common.format.parquet.delegate;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheOutputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoTwitterInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoTwitterOutputFormat;

public class DelegateFormatFactory {

  private DelegateFormatFactory() {
    // static methods only
  }

  public static IPentahoParquetInputFormat getInputFormatInstance( NamedCluster namedCluster )  {
    if ( shimUsesTwitterLibs( namedCluster ) ) {
      return new PentahoTwitterInputFormat( namedCluster );
    } else {
      return new PentahoApacheInputFormat( namedCluster );
    }
  }

  public static IPentahoParquetOutputFormat getOutputFormatInstance( NamedCluster namedCluster )  {
    if ( shimUsesTwitterLibs( namedCluster ) ) {
      return new PentahoTwitterOutputFormat();
    } else {
      return new PentahoApacheOutputFormat( namedCluster );
    }
  }

  private static boolean shimUsesTwitterLibs( NamedCluster namedCluster ) {
    String shimIdentifier = namedCluster == null ? "" : namedCluster.getShimIdentifier();
    return ( shimIdentifier.startsWith( "cdh" ) && !shimIdentifier.startsWith( "cdh6" ) )
      || ( shimIdentifier.startsWith( "mapr" ) && !shimIdentifier.equals( "mapr60" ) );
  }
}
