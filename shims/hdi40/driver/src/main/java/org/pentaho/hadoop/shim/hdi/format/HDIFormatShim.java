/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2022 by Hitachi Vantara : http://www.pentaho.com
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