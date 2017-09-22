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
package org.pentaho.hadoop.shim.common.format;


import org.apache.hadoop.fs.Path;
//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI"
import org.apache.parquet.hadoop.ParquetInputSplit;
//#endif
//#if shim_type=="CDH" || shim_type=="MAPR"
//$import parquet.hadoop.ParquetInputSplit;
//#endif
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.pentaho.di.core.util.Assert;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.format.parquet.PentahoInputSplitImpl;

public class PentahoParquetInputFormatTest {

  @Test
  public void createRecordReader() throws Exception {

    String parquetFilePath = getClass().getClassLoader().getResource( "sample.pqt" ).toExternalForm();

    PentahoParquetInputFormat pentahoParquetInputFormat = new PentahoParquetInputFormat();
    pentahoParquetInputFormat.setInputFile( getClass().getClassLoader().getResource( "sample.pqt" ).toExternalForm() );
    SchemaDescription schema = pentahoParquetInputFormat.readSchema( parquetFilePath );

    pentahoParquetInputFormat.setSchema( schema );

    ParquetInputSplit parquetInputSplit = Mockito.spy( ParquetInputSplit.class );
    Whitebox.setInternalState( parquetInputSplit, "rowGroupOffsets", new long[] { 4 } );
    Whitebox.setInternalState( parquetInputSplit, "file", new Path( parquetFilePath ) );

    PentahoInputSplitImpl pentahoInputSplit = new PentahoInputSplitImpl( parquetInputSplit );

    IPentahoInputFormat.IPentahoRecordReader recordReader =
      pentahoParquetInputFormat.createRecordReader( pentahoInputSplit );

    Assert.assertNotNull( recordReader, "recordReader should NOT be null!" );
    Assert.assertTrue( recordReader instanceof IPentahoInputFormat.IPentahoRecordReader,
      "recordReader should be instance of IPentahoInputFormat.IPentahoRecordReader" );
  }
}
