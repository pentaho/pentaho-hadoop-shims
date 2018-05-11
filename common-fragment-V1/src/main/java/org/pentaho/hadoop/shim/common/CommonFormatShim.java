/*! ******************************************************************************
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
package org.pentaho.hadoop.shim.common;

import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcInputFormat;
import org.pentaho.hadoop.shim.common.format.orc.PentahoOrcOutputFormat;
import org.pentaho.hadoop.shim.common.format.avro.PentahoAvroInputFormat;
import org.pentaho.hadoop.shim.common.format.avro.PentahoAvroOutputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.DelegateFormatFactory;
import org.pentaho.hadoop.shim.spi.FormatShim;

public class CommonFormatShim implements FormatShim {

  @Override
  public <T extends IPentahoInputFormat> T createInputFormat( Class<T> type, NamedCluster namedCluster )
    throws Exception {
    if ( type.isAssignableFrom( IPentahoParquetInputFormat.class ) ) {
      return (T) DelegateFormatFactory.getInputFormatInstance( namedCluster );
    } else if ( type.isAssignableFrom( IPentahoAvroInputFormat.class ) ) {
      return (T) new PentahoAvroInputFormat( namedCluster );
    } else if ( type.isAssignableFrom( IPentahoOrcInputFormat.class ) ) {
      return (T) new PentahoOrcInputFormat( namedCluster );
    }
    throw new IllegalArgumentException( "Not supported scheme format" );
  }

  @Override
  public <T extends IPentahoOutputFormat> T createOutputFormat( Class<T> type, NamedCluster namedCluster )
    throws Exception {
    if ( type.isAssignableFrom( IPentahoParquetOutputFormat.class ) ) {
      return (T) DelegateFormatFactory.getOutputFormatInstance( namedCluster );
    } else if ( type.isAssignableFrom( IPentahoAvroOutputFormat.class ) ) {
      return (T) new PentahoAvroOutputFormat();
    } else if ( type.isAssignableFrom( IPentahoOrcOutputFormat.class ) ) {
      return (T) new PentahoOrcOutputFormat();
    }
    throw new IllegalArgumentException( "Not supported scheme format" );
  }

  @Override
  public ShimVersion getVersion() {
    return null;
  }
}
