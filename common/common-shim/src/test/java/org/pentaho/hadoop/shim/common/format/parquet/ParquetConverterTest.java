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
package org.pentaho.hadoop.shim.common.format.parquet;

import org.apache.hadoop.fs.Path;
//#if shim_type=="HDP" || shim_type=="EMR" || shim_type=="HDI" || shim_name=="mapr60" || shim_name=="cdh61"
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
//#endif
//#if shim_type=="CDH" && shim_name!="cdh61" || shim_type=="MAPR" && shim_name!="mapr60"
//$import parquet.format.converter.ParquetMetadataConverter;
//$import parquet.hadoop.ParquetFileReader;
//$import parquet.hadoop.metadata.ParquetMetadata;
//$import parquet.schema.MessageType;
//#endif
import org.junit.Assert;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;

import java.net.URL;
import java.nio.file.Paths;
import java.util.List;


public class ParquetConverterTest {

  private static String PARQUET_FILE = "sample.pqt";
  private static URL urlTestResources;

  @Test
  public void convertParquetSchemaToKettleWithTwoValidRows() throws Exception {

    int pentahoValueMetaTypeFirstRow = 2;
    int pentahoValueMetaTypeSecondRow = 5;

    List<IParquetInputField> fields = ParquetUtils
      .createSchema( pentahoValueMetaTypeFirstRow, pentahoValueMetaTypeSecondRow );
    String expectedKettleSchema = new ParquetInputFieldList( fields ).marshall();

    urlTestResources = Thread.currentThread().getContextClassLoader().getResource( PARQUET_FILE );

    ConfigurationProxy conf = new ConfigurationProxy();
    conf.set( "fs.defaultFS", "file:///" );
    ParquetMetadata meta = ParquetFileReader
      .readFooter( conf, new Path( Paths.get( urlTestResources.toURI() ).toString() ),
        ParquetMetadataConverter.NO_FILTER );
    MessageType schema = meta.getFileMetaData().getSchema();

    List<IParquetInputField> kettleSchema = ParquetConverter.buildInputFields( schema );
    String marshallKettleSchema = new ParquetInputFieldList( kettleSchema ).marshall();
    Assert.assertEquals( marshallKettleSchema, expectedKettleSchema );
  }
}
