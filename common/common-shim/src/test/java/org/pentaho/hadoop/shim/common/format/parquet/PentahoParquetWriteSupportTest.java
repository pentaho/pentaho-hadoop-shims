/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.parquet;

import org.apache.hadoop.conf.Configuration;
//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
import org.apache.parquet.hadoop.api.WriteSupport;
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR"
//$import parquet.hadoop.api.WriteSupport;
//#endif
import org.pentaho.di.core.util.Assert;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.format.ParquetUtils;


public class PentahoParquetWriteSupportTest {

  @Test( expected = RuntimeException.class )
  public void initParquetWriteSupportWhenSchemaIsNull() {
    SchemaDescription schema = new SchemaDescription();
    PentahoParquetWriteSupport writeSupport = new PentahoParquetWriteSupport( schema );

    Configuration conf = new Configuration();
    conf.set( "fs.defaultFS", "file:///" );

    writeSupport.init( conf );
  }

  @Test
  public void initParquetWriteSupportWhenSchemaIsNotNull() {

    int pentahoValueMetaTypeFirstRow = 2;
    boolean allowNullFirstRow = false;
    int pentahoValueMetaTypeSecondRow = 5;
    boolean allowNullSecondRow = false;

    String schemaFromString = ParquetUtils
      .createSchema( pentahoValueMetaTypeFirstRow, allowNullFirstRow, pentahoValueMetaTypeSecondRow,
        allowNullSecondRow ).marshall();

    SchemaDescription schema = SchemaDescription.unmarshall( schemaFromString );
    PentahoParquetWriteSupport writeSupport = new PentahoParquetWriteSupport( schema );

    Configuration conf = new Configuration();
    conf.set( "fs.defaultFS", "file:///" );

    WriteSupport.WriteContext writeContext = writeSupport.init( conf );

    Assert.assertNotNull( writeContext );
  }
}