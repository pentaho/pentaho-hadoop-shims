/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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
      return new PentahoApacheOutputFormat();
    }
  }

  private static boolean shimUsesTwitterLibs( NamedCluster namedCluster ) {
    String shimIdentifier = namedCluster == null ? "" : namedCluster.getShimIdentifier();
    return ( shimIdentifier.startsWith( "cdh" ) && !shimIdentifier.startsWith( "cdh6" ) )
      || ( shimIdentifier.startsWith( "mapr" ) && !shimIdentifier.equals( "mapr60" ) );
  }
}
