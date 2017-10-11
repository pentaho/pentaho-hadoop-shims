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
package org.pentaho.hadoop.shim.common.format;


import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.di.core.util.Assert;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;

import java.nio.file.Files;

public class PentahoParquetOutputFormatTest {

  private static PentahoParquetOutputFormat pentahoParquetOutputFormat;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    pentahoParquetOutputFormat = new PentahoParquetOutputFormat();
  }

  //#if shim_type!="MAPR"
  @Test
  public void createRecordWriterWhenSchemaAndPathIsNotNull() throws Exception {

    String tempFile = Files.createTempDirectory( "parquet" ).toUri().toString();
    pentahoParquetOutputFormat.setOutputFile( tempFile.toString() + "test", true );
    pentahoParquetOutputFormat.setSchema( ParquetUtils.createSchema() );

    IPentahoOutputFormat.IPentahoRecordWriter recordWriter =
      pentahoParquetOutputFormat.createRecordWriter();

    Assert.assertNotNull( recordWriter, "recordWriter should NOT be null!" );
    Assert.assertTrue( recordWriter instanceof IPentahoOutputFormat.IPentahoRecordWriter,
      "recordWriter should be instance of IPentahoInputFormat.IPentahoRecordReader" );
  }
  //#endif

  @Test( expected = RuntimeException.class )
  public void createRecordWriterWhenSchemaIsNull() throws Exception {

    SchemaDescription schema = Mockito.mock( SchemaDescription.class );

    String tempFile = Files.createTempDirectory( "parquet" ).toUri().toString();

    PentahoParquetOutputFormat pentahoParquetOutputFormat =
      new PentahoParquetOutputFormat();

    pentahoParquetOutputFormat.setOutputFile( tempFile.toString() + "test1", true );
    pentahoParquetOutputFormat.setSchema( schema );

    pentahoParquetOutputFormat.createRecordWriter();
  }

  @Test( expected = RuntimeException.class )
  public void createRecordWriterWhenPathIsNull() throws Exception {

    String tempFile = null;

    PentahoParquetOutputFormat pentahoParquetOutputFormat =
      new PentahoParquetOutputFormat();

    pentahoParquetOutputFormat.setOutputFile( tempFile, true );
    pentahoParquetOutputFormat.setSchema( ParquetUtils.createSchema() );

    pentahoParquetOutputFormat.createRecordWriter();
  }
}
