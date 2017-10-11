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
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR"
//$import parquet.hadoop.api.InitContext;
//$import parquet.hadoop.api.ReadSupport;
//#endif


import org.junit.Test;
import org.pentaho.di.core.util.Assert;
import org.pentaho.hadoop.shim.common.format.ParquetUtils;

public class PentahoParquetReadSupportTest {

  @Test
  public void initParquetReadSupportWhenSchemaIsNotNull() {

    int pentahoValueMetaTypeFirstRow = 2;
    boolean allowNullFirstRow = false;
    int pentahoValueMetaTypeSecondRow = 5;
    boolean allowNullSecondRow = false;

    String schemaFromString = ParquetUtils
      .createSchema( pentahoValueMetaTypeFirstRow, allowNullFirstRow, pentahoValueMetaTypeSecondRow,
        allowNullSecondRow ).marshall();

    PentahoParquetReadSupport readSupport = new PentahoParquetReadSupport();

    Configuration conf = new Configuration();
    conf.set( "fs.defaultFS", "file:///" );
    conf.set( "PentahoParquetSchema", schemaFromString );

    InitContext context = new InitContext( conf, null, null );
    ReadSupport.ReadContext readContext = readSupport.init( context );

    Assert.assertNotNull( readContext );
  }

  @Test( expected = RuntimeException.class )
  public void initParquetReadSupportWhenSchemaIsNull() {

    PentahoParquetReadSupport readSupport = new PentahoParquetReadSupport();
    Configuration conf = new Configuration();
    conf.set( "fs.defaultFS", "file:///" );
    InitContext context = new InitContext( conf, null, null );
    readSupport.init( context );
  }

  @Test( expected = RuntimeException.class )
  public void initParquetReadSupportWhenSchemaIsEmptyString() {

    PentahoParquetReadSupport readSupport = new PentahoParquetReadSupport();
    Configuration conf = new Configuration();
    conf.set( "fs.defaultFS", "file:///" );
    conf.set( "PentahoParquetSchema", " " );

    InitContext context = new InitContext( conf, null, null );
    readSupport.init( context );
  }
}
