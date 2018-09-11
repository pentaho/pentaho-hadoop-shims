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

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import org.pentaho.hadoop.shim.common.format.parquet.ParquetInputFieldList;
import org.pentaho.hadoop.shim.common.format.parquet.ParquetUtils;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.ParquetConverter;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;



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
