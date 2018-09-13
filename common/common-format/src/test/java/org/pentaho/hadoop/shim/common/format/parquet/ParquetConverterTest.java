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
package org.pentaho.hadoop.shim.common.format.parquet;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;


@RunWith(Parameterized.class)
public class ParquetConverterTest {

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] { { "APACHE" }, { "TWITTER" } });
  }

  @Parameterized.Parameter
  public String provider;

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
    List<IParquetInputField> kettleSchema = null;

    switch( provider ) {
      case "APACHE":
        org.apache.parquet.hadoop.metadata.ParquetMetadata apacheMeta = org.apache.parquet.hadoop.ParquetFileReader
                .readFooter( conf, new Path( Paths.get( urlTestResources.toURI() ).toString() ),
                        org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER );
        org.apache.parquet.schema.MessageType apacheSchema = apacheMeta.getFileMetaData().getSchema();
        kettleSchema = org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.ParquetConverter.buildInputFields( apacheSchema );
        break;
      case "TWITTER":
        parquet.hadoop.metadata.ParquetMetadata twitterMeta = parquet.hadoop.ParquetFileReader
                .readFooter( conf, new Path( Paths.get( urlTestResources.toURI() ).toString() ),
                        parquet.format.converter.ParquetMetadataConverter.NO_FILTER );
        parquet.schema.MessageType twitterSchema = twitterMeta.getFileMetaData().getSchema();
        kettleSchema = org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.ParquetConverter.buildInputFields( twitterSchema );
        break;
      default:
        Assert.fail("Invalid provider name used.");
    }

    String marshallKettleSchema = new ParquetInputFieldList( kettleSchema ).marshall();
    Assert.assertEquals( marshallKettleSchema, expectedKettleSchema );
  }
}
