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
package org.pentaho.hadoop.shim.common;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IPentahoAvroOutputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat.IPentahoRecordReader;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat.IPentahoRecordWriter;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
import org.pentaho.hadoop.shim.common.format.ParquetUtils;
import org.pentaho.hadoop.shim.common.format.PentahoParquetInputFormat;
import org.pentaho.hadoop.shim.common.format.PentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.common.format.avro.AvroInputField;
import org.pentaho.hadoop.shim.common.format.avro.AvroOutputField;
import org.pentaho.hadoop.shim.common.format.avro.PentahoAvroInputFormat;
import org.pentaho.hadoop.shim.common.format.avro.PentahoAvroOutputFormat;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created by Vasilina_Terehova on 7/27/2017.
 */
public class CommonFormatShimTestIT {
  //#if shim_type!="MAPR"
  @Test
  public void testParquetReadSuccessLocalFileSystem() throws Exception {

    List<String> expectedRows = new ArrayList<>();
    expectedRows.add( "Alex Blum;15" );
    expectedRows.add( "Tom Falls;24" );

    PentahoParquetInputFormat pentahoParquetInputFormat = new PentahoParquetInputFormat();
    pentahoParquetInputFormat
      .setInputFile( getClass().getClassLoader().getResource( "sample.pqt" ).toExternalForm() );
    pentahoParquetInputFormat.setSchema( ParquetUtils.createSchema( ValueMetaInterface.TYPE_INTEGER ) );
    IPentahoRecordReader recordReader =
      pentahoParquetInputFormat.createRecordReader( pentahoParquetInputFormat.getSplits().get( 0 ) );

    List<String> dataSampleRows = new ArrayList<>();

    recordReader.forEach( rowMetaAndData -> {
      dataSampleRows.add( rowMetaAndData.getData()[ 0 ].toString() + ";" + rowMetaAndData.getData()[ 1 ].toString() );
    } );

    assertEquals( expectedRows, dataSampleRows );
  }

  @Test
  public void testParquetWriteSuccessLocalFileSystem() throws Exception {
    final String PARQUET_FILE_NAME = "test.parquet";

    String tempFile = Files.createTempDirectory( "parquet" ).toUri().toString();

    ConfigurationProxy jobConfiguration = new ConfigurationProxy();
    jobConfiguration.set( FileOutputFormat.OUTDIR, tempFile );

    String parquetFilePath = jobConfiguration.get( FileOutputFormat.OUTDIR ) + PARQUET_FILE_NAME;

    PentahoParquetOutputFormat pentahoParquetOutputFormat =
      new PentahoParquetOutputFormat();

    pentahoParquetOutputFormat.setOutputFile( parquetFilePath, true );

    pentahoParquetOutputFormat.setSchema( ParquetUtils.createSchema( ValueMetaInterface.TYPE_INTEGER ) );

    IPentahoRecordWriter recordWriter = pentahoParquetOutputFormat.createRecordWriter();
    RowMetaAndData rowInput = new RowMetaAndData();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "Name" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Age" ) );
    rowInput.setRowMeta( rowMeta );

    rowInput.setData( new Object[] { "Andrey", "11" } );

    recordWriter.write( rowInput );
    recordWriter.close();

    IPentahoRecordReader recordReader = readCreatedParquetFile( parquetFilePath );

    Object[] rowInputArr =
      new Object[] { rowInput.getData()[ 0 ].toString(), Long.parseLong( rowInput.getData()[ 1 ].toString() ) };

    recordReader.forEach(
      rowMetaAndData -> org.junit.Assert.assertArrayEquals( rowMetaAndData.getData(), rowInputArr ) );
  }

  private IPentahoRecordReader readCreatedParquetFile( String parquetFilePath )
    throws Exception {
    PentahoParquetInputFormat pentahoParquetInputFormat = new PentahoParquetInputFormat();

    pentahoParquetInputFormat.setInputFile( parquetFilePath );
    SchemaDescription schema = pentahoParquetInputFormat.readSchema( parquetFilePath );

    pentahoParquetInputFormat.setSchema( schema );
    IPentahoRecordReader recordReader =
      pentahoParquetInputFormat.createRecordReader( pentahoParquetInputFormat.getSplits().get( 0 ) );

    return recordReader;
  }

  @Test
  public void testAvroReadLocalFileSystem() throws Exception {
    List<String> expectedRows = Arrays.asList( "John;4074549921", "Leslie;4079302194" );
    PentahoAvroInputFormat avroInputFormat = new PentahoAvroInputFormat();
    avroInputFormat.setInputSchemaFile( getFilePath( "/sample-schema.avro" ) );
    avroInputFormat.setInputFile( getFilePath( "/sample-data.avro" ) );

    List<AvroInputField> inputFields = new ArrayList<AvroInputField>();

    AvroInputField avroInputField = new AvroInputField();
    avroInputField.setAvroFieldName( "FirstName" );
    avroInputField.setPentahoFieldName( "FirstName" );
    avroInputField.setAvroType( AvroSpec.DataType.STRING );
    avroInputField.setPentahoType( ValueMetaInterface.TYPE_STRING );
    inputFields.add( avroInputField );

    avroInputField = new AvroInputField();
    avroInputField.setAvroFieldName( "Phone" );
    avroInputField.setPentahoFieldName( "Phone" );
    avroInputField.setAvroType( AvroSpec.DataType.STRING );
    avroInputField.setPentahoType( ValueMetaInterface.TYPE_STRING );
    inputFields.add( avroInputField );

    avroInputFormat.setInputFields( inputFields );

    IPentahoRecordReader recordReader = avroInputFormat.createRecordReader( null );
    List<String> dataSampleRows = new ArrayList<>();
    recordReader.forEach( rowMetaAndData -> {
      dataSampleRows.add( String.join( ";", rowMetaAndData.getData()[0].toString(), rowMetaAndData.getData()[1].toString() ) );
    } );
    assertEquals( expectedRows, dataSampleRows );
  }

  @Test
  public void testAvroWriteLocalFileSystem() throws Exception {
    String tempDir = Files.createTempDirectory( "avro" ).toUri().toString();

    List<AvroOutputField> outputFields = new ArrayList<AvroOutputField>();

    AvroOutputField avroOutputField = new AvroOutputField();
    avroOutputField.setAvroFieldName( "name" );
    avroOutputField.setPentahoFieldName( "name" );
    avroOutputField.setAllowNull( false );
    avroOutputField.setDefaultValue( null );
    avroOutputField.setAvroType( AvroSpec.DataType.STRING );
    outputFields.add( avroOutputField );

    avroOutputField = new AvroOutputField();
    avroOutputField.setAvroFieldName( "phone" );
    avroOutputField.setPentahoFieldName( "phone" );
    avroOutputField.setAllowNull( false );
    avroOutputField.setDefaultValue( null );
    avroOutputField.setAvroType( AvroSpec.DataType.STRING );
    outputFields.add( avroOutputField );

    PentahoAvroOutputFormat outputFormat = new PentahoAvroOutputFormat();
    outputFormat.setFields( outputFields );
    outputFormat.setSchemaFilename(  tempDir + "/avro-schema.out" );
    outputFormat.setOutputFile( tempDir + "/avro.out" );
    outputFormat.setNameSpace( "nameSpace" );
    outputFormat.setRecordName( "recordName" );
    outputFormat.setCompression( IPentahoAvroOutputFormat.COMPRESSION.UNCOMPRESSED );
    IPentahoRecordWriter recordWriter = outputFormat.createRecordWriter();

    RowMetaAndData row = new RowMetaAndData();
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "name" ) );
    rowMeta.addValueMeta( new ValueMetaString( "phone" ) );
    row.setRowMeta( rowMeta );

    row.setData( new Object[] { "Alice", "987654321" } );
    recordWriter.write( row );
    recordWriter.close();

    PentahoAvroInputFormat avroInputFormat = new PentahoAvroInputFormat();
    List<AvroInputField> inputFields = new ArrayList<AvroInputField>();

    AvroInputField avroInputField = new AvroInputField();
    avroInputField.setAvroFieldName( "name" );
    avroInputField.setPentahoFieldName( "name" );
    avroInputField.setAvroType( AvroSpec.DataType.STRING );
    avroInputField.setPentahoType( ValueMetaInterface.TYPE_STRING );
    inputFields.add( avroInputField );

    avroInputField = new AvroInputField();
    avroInputField.setAvroFieldName( "phone" );
    avroInputField.setPentahoFieldName( "phone" );
    avroInputField.setAvroType( AvroSpec.DataType.STRING );
    avroInputField.setPentahoType( ValueMetaInterface.TYPE_STRING );
    inputFields.add( avroInputField );

    avroInputFormat.setInputFields( inputFields );

    avroInputFormat.setInputSchemaFile( tempDir + "/avro-schema.out" );
    avroInputFormat.setInputFile( tempDir + "/avro.out" );
    avroInputFormat.setInputFields( inputFields );
    IPentahoRecordReader recordReader = avroInputFormat.createRecordReader( null );
    recordReader.forEach( rowMetaAndData ->
      assertArrayEquals( new Object[] { "Alice", "987654321" }, new Object[] { rowMetaAndData.getData()[0].toString(),
        rowMetaAndData.getData()[1].toString() } ) );

  }

  private String getFilePath( String file ) {
    return getClass().getResource( file ).getPath();
  }

  //#endif
}
