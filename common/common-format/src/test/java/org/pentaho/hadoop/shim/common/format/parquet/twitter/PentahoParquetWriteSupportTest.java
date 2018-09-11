/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.parquet.twitter;

import org.apache.hadoop.conf.Configuration;
import org.pentaho.di.core.util.Assert;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;

//Twitter imports
import org.pentaho.hadoop.shim.common.format.parquet.ParquetUtils;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoParquetWriteSupport;
import parquet.hadoop.api.WriteSupport;

public class PentahoParquetWriteSupportTest {

  @Test( expected = RuntimeException.class )
  public void initParquetWriteSupportWhenSchemaIsNull() {
    PentahoParquetWriteSupport writeSupport = new PentahoParquetWriteSupport( null );

    Configuration conf = new Configuration();
    conf.set( "fs.defaultFS", "file:///" );

    writeSupport.init( conf );
  }

  @Test
  public void initParquetWriteSupportWhenSchemaIsNotNull() {

    PentahoParquetWriteSupport writeSupport = new PentahoParquetWriteSupport( ParquetUtils.createOutputFields( ParquetSpec.DataType.UTF8, false, ParquetSpec.DataType.INT_64, false ) );

    Configuration conf = new Configuration();
    conf.set( "fs.defaultFS", "file:///" );

    WriteSupport.WriteContext writeContext = writeSupport.init( conf );

    Assert.assertNotNull( writeContext );
  }
}
