/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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
import org.pentaho.hadoop.shim.api.format.IPentahoAvroInput;
import org.pentaho.hadoop.shim.api.format.PentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.PentahoOutputFormat;
import org.pentaho.hadoop.shim.common.format.PentahoParquetInputFormat;
import org.pentaho.hadoop.shim.common.format.PentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.common.format.avro.PentahoAvroInputFormat;
import org.pentaho.hadoop.shim.spi.FormatShim;

public class CommonFormatShim implements FormatShim {

  @Override
  public PentahoInputFormat createInputFormat( FormatType type ) {
    if ( type == FormatType.PARQUET ) {
      return new PentahoParquetInputFormat();
    }
    throw new IllegalArgumentException( "Not supported scheme format" );
  }

  @Override
  public PentahoOutputFormat createOutputFormat( FormatType type ) {
    if ( type == FormatType.PARQUET ) {
      return new PentahoParquetOutputFormat();
    }
    throw new IllegalArgumentException( "Not supported scheme format" );
  }

  @Override
  public ShimVersion getVersion() {
    return null;
  }

  @Override
  public IPentahoAvroInput createAvroInput() {
    // TODO Auto-generated method stub
    return new PentahoAvroInputFormat();
  }
}
