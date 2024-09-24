/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019-2020 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat.IPentahoRecordReader;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat.IPentahoRecordWriter;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetOutputFormat;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;
import org.pentaho.hadoop.shim.common.format.parquet.ParquetUtils;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheOutputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoTwitterInputFormat;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoTwitterOutputFormat;

import java.io.File;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Created by Vasilina_Terehova on 7/27/2017.
 */
@RunWith( Parameterized.class )
public class CommonFormatShimTest {

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList( new Object[][] { { "APACHE" }, { "TWITTER" } } );
  }

  @Parameterized.Parameter
  public String provider;

  final String jsonDatumData = "{\"parentInt\": 1, \"parentBool\": true, \"parentLong\": 2, \"parentFloat\": 3.0, "
    + "\"parentDouble\": 4.0, \"parentString\": \"string1\", \"parentStringMap\": {\"key1\": \"string2\", \"key2\":"
    + " \"string3\"}, \"parentIntMap\": {\"key3\": 5, \"key4\": 6}, \"parentStringArray\": [\"string4\", "
    + "\"string5\"],\"parentIntArray\": [7, 8], \"childData\": {\"childInt\": 9, \"childBool\": false, "
    + "\"childLong\": 10, \"childFloat\": 11.0, \"childDouble\": 12.0, \"childString\": \"string6\", "
    + "\"childStringMap\": {\"key5\": \"string7\", \"key6\": \"string8\"}, \"childIntMap\": {\"key7\": 13, "
    + "\"key8\": 14 }, \"childStringArray\": [\"string9\", \"string10\"], \"childIntArray\": [15, 16]}}";
  final String jsonSchema =
    "{\"namespace\": \"example.avro\",\"name\": \"ParentData\",\"type\": \"record\",\"fields\": "
      + "[{\"name\": \"parentInt\", \"type\": \"int\"},{\"name\":\"parentBool\", \"type\": \"boolean\"},{\"name\": "
      + "\"parentLong\", \"type\": \"long\"},{\"name\": \"parentFloat\", \"type\": \"float\"},{\"name\": "
      + "\"parentDouble\", \"type\": \"double\"},{\"name\": \"parentString\", \"type\": \"string\"},{\"name\": "
      + "\"parentStringMap\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},{\"name\": \"parentIntMap\", "
      + "\"type\": {\"type\": \"map\", \"values\": \"int\"}},{\"name\": \"parentStringArray\", \"type\": {\"type\": "
      + "\"array\", \"items\": \"string\"}},{\"name\": \"parentIntArray\", \"type\": {\"type\": \"array\", \"items\":"
      + " \"int\"}},{\"name\": \"childData\",\"type\": {\"namespace\": \"example.avro\",\"type\" : \"record\","
      + "\"name\" : \"ChildData\",\"fields\" : [{\"name\": \"childInt\", \"type\": \"int\"},{\"name\": \"childBool\","
      + " \"type\": \"boolean\"},{\"name\": \"childLong\", \"type\": \"long\"},{\"name\": \"childFloat\", \"type\": "
      + "\"float\"},{\"name\": \"childDouble\", \"type\": \"double\"},{\"name\": \"childString\", \"type\": "
      + "\"string\"},{\"name\": \"childStringMap\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},{\"name\":"
      + " \"childIntMap\", \"type\": {\"type\": \"map\", \"values\": \"int\"}},{\"name\": \"childStringArray\", "
      + "\"type\": {\"type\": \"array\", \"items\": \"string\"}},{\"name\": \"childIntArray\", \"type\": {\"type\": "
      + "\"array\", \"items\": \"int\"}}]}}]}";


  @Before
  public void before() throws KettleException {
    KettleEnvironment.init();
  }


  @Test
  public void testParquetReadSuccessLocalFileSystem() throws Exception {

    List<String> expectedRows = new ArrayList<>();
    expectedRows.add( "Alex Blum;15" );
    expectedRows.add( "Tom Falls;24" );

    IPentahoParquetInputFormat pentahoParquetInputFormat = null;
    NamedCluster namedCluster = mock( NamedCluster.class );
    switch ( provider ) {
      case "APACHE":
        pentahoParquetInputFormat = new PentahoApacheInputFormat( namedCluster );
        break;
      case "TWITTER":
        pentahoParquetInputFormat = new PentahoTwitterInputFormat( namedCluster );
        break;
      default:
        Assert.fail( "Invalid provider name used." );
    }

    pentahoParquetInputFormat
      .setInputFile(
        Objects.requireNonNull( getClass().getClassLoader().getResource( "sample.pqt" ) ).toExternalForm() );
    pentahoParquetInputFormat.setSchema( ParquetUtils.createSchema( ValueMetaInterface.TYPE_INTEGER ) );
    IPentahoRecordReader recordReader =
      pentahoParquetInputFormat.createRecordReader( pentahoParquetInputFormat.getSplits().get( 0 ) );

    List<String> dataSampleRows = new ArrayList<>();

    recordReader.forEach( rowMetaAndData -> dataSampleRows
      .add( rowMetaAndData.getData()[ 0 ].toString() + ";" + rowMetaAndData.getData()[ 1 ].toString() ) );

    assertEquals( expectedRows, dataSampleRows );
  }

  @Test
  public void testParquetWriteSuccessLocalFileSystem() throws Exception {
    final String PARQUET_FILE_NAME = "test.parquet";

    String tempFile = Files.createTempDirectory( "parquet" ).toUri().toString();

    ConfigurationProxy jobConfiguration = new ConfigurationProxy();
    jobConfiguration.set( FileOutputFormat.OUTDIR, tempFile );

    String parquetFilePath = jobConfiguration.get( FileOutputFormat.OUTDIR ) + PARQUET_FILE_NAME;

    IPentahoParquetOutputFormat pentahoParquetOutputFormat = null;
    switch ( provider ) {
      case "APACHE":
        pentahoParquetOutputFormat = new PentahoApacheOutputFormat();
        break;
      case "TWITTER":
        pentahoParquetOutputFormat = new PentahoTwitterOutputFormat();
        break;
      default:
        org.junit.Assert.fail( "Invalid provider name used." );
    }

    pentahoParquetOutputFormat.setOutputFile( parquetFilePath, true );

    pentahoParquetOutputFormat.setFields( ParquetUtils.createOutputFields( ParquetSpec.DataType.INT_64 ) );

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
    PentahoApacheInputFormat pentahoParquetInputFormat = new PentahoApacheInputFormat( mock( NamedCluster.class ) );

    pentahoParquetInputFormat.setInputFile( parquetFilePath );
    List<IParquetInputField> schema = pentahoParquetInputFormat.readSchema( parquetFilePath );

    pentahoParquetInputFormat.setSchema( schema );

    return pentahoParquetInputFormat.createRecordReader( pentahoParquetInputFormat.getSplits().get( 0 ) );
  }

  private String getFilePath( String file ) {
    return getClass().getResource( file ).getPath();
  }



}
