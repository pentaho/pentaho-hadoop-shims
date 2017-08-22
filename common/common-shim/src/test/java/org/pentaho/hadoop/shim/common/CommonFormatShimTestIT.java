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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.hadoop.shim.api.format.PentahoRecordReader;
import org.pentaho.hadoop.shim.api.format.PentahoRecordWriter;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.format.PentahoParquetInputFormat;
import org.pentaho.hadoop.shim.common.format.PentahoParquetOutputFormat;

/**
 * Created by Vasilina_Terehova on 7/27/2017.
 */
public class CommonFormatShimTestIT {

  @Test
  public void testParquetReadSuccessLocalFileSystem() throws IOException, InterruptedException {
    SchemaDescription schemaDescription = makeScheme();

    PentahoParquetInputFormat pentahoParquetInputFormat = new PentahoParquetInputFormat();
    pentahoParquetInputFormat.setInputFile( CommonFormatShimTestIT.class.getClassLoader().getResource( "sample.pqt" )
        .toExternalForm() );
    pentahoParquetInputFormat.setSchema( schemaDescription );
    PentahoRecordReader recordReader =
        pentahoParquetInputFormat.createRecordReader( pentahoParquetInputFormat.getSplits().get( 0 ) );
    recordReader.forEach( rowMetaAndData -> {
      RowMetaInterface rowMeta = rowMetaAndData.getRowMeta();
      for ( String fieldName : rowMeta.getFieldNames() ) {
        try {
          System.out.println( fieldName + " " + rowMetaAndData.getString( fieldName, "" ) );
        } catch ( KettleValueException e ) {
          e.printStackTrace();
        }
      }
    } );
  }

  @Test
  public void testParquetReadSuccessHdfsFileSystem() throws IOException, InterruptedException {

    SchemaDescription schemaDescription = makeScheme();
    PentahoParquetInputFormat pentahoParquetInputFormat =
      new PentahoParquetInputFormat(  );
    pentahoParquetInputFormat.setInputFile( "hdfs://svqxbdcn6cdh510n1.pentahoqa.com:8020/user/devuser/parquet" );
    PentahoRecordReader recordReader =
      pentahoParquetInputFormat.createRecordReader( pentahoParquetInputFormat.getSplits().get( 0 ) );
    recordReader.forEach( rowMetaAndData -> {
      RowMetaInterface rowMeta = rowMetaAndData.getRowMeta();
      for ( String fieldName : rowMeta.getFieldNames() ) {
        try {
          System.out.println( fieldName + " " + rowMetaAndData.getString( fieldName, "" ) );
        } catch ( KettleValueException e ) {
          e.printStackTrace();
        }
      }
    } );

  }


  @Test
  public void testParquetReadSuccessLocalFileSystemAlex() throws IOException, InterruptedException {
    try {
      SchemaDescription schemaDescription = makeScheme();

      ConfigurationProxy jobConfiguration = new ConfigurationProxy();
      jobConfiguration.set( FileInputFormat.INPUT_DIR, CommonFormatShimTestIT.class.getClassLoader().getResource(
        "sample.pqt" ).getFile() );
      PentahoParquetInputFormat pentahoParquetInputFormat =
        new PentahoParquetInputFormat( );
      PentahoRecordReader recordReader =
        pentahoParquetInputFormat.createRecordReader( pentahoParquetInputFormat.getSplits().get( 0 ) );
      recordReader.forEach( rowMetaAndData -> {
        RowMetaInterface rowMeta = rowMetaAndData.getRowMeta();
        for ( String fieldName : rowMeta.getFieldNames() ) {
          try {
            System.out.println( fieldName + " " + rowMetaAndData.getString( fieldName, "" ) );
          } catch ( KettleValueException e ) {
            e.printStackTrace();
          }
        }
      } );
    } catch ( Throwable ex ) {
      ex.printStackTrace();
    }
  }

  @Test
  public void testParquetWriteSuccessLocalFileSystem() throws IOException, InterruptedException {
    try {
      SchemaDescription schemaDescription = makeScheme();
      Path tempFile = Files.createTempDirectory( "parquet" );

      ConfigurationProxy jobConfiguration = new ConfigurationProxy();
      jobConfiguration.set( FileOutputFormat.OUTDIR, tempFile.toString() );
      PentahoParquetOutputFormat pentahoParquetOutputFormat =
        new PentahoParquetOutputFormat(  );
      PentahoRecordWriter recordWriter =
        pentahoParquetOutputFormat.createRecordWriter();
      RowMetaAndData
        row = new RowMetaAndData();
      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta( new ValueMetaString( "Name" ) );
      rowMeta.addValueMeta( new ValueMetaString( "Age" ) );
      row.setRowMeta( rowMeta );
      row.setData( new Object[] { "Andrey", "11 years" } );

      //for now integer doesn't work! for read
      recordWriter.write( row );
      recordWriter.close();
    } catch ( Throwable ex ) {
      ex.printStackTrace();
    }
  }

  private SchemaDescription makeScheme() {
    SchemaDescription s = new SchemaDescription();
    s.addField( s.new Field( "b", "Name", ValueMetaInterface.TYPE_STRING, true ) );
    s.addField( s.new Field( "c", "Age", ValueMetaInterface.TYPE_STRING, true ) );
    return s;
  }
}
