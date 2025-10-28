/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.hadoop.shim.common.format.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
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

@RunWith( Parameterized.class )
public class ParquetConverterTest {

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList( new Object[][] { { "APACHE" }, { "TWITTER" } } );
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

    switch ( provider ) {
      case "APACHE":
        org.apache.parquet.hadoop.metadata.ParquetMetadata apacheMeta = org.apache.parquet.hadoop.ParquetFileReader
          .readFooter( conf, new Path( Paths.get( urlTestResources.toURI() ).toString() ),
            org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER );
        org.apache.parquet.schema.MessageType apacheSchema = apacheMeta.getFileMetaData().getSchema();
        kettleSchema = org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.ParquetConverter
          .buildInputFields( apacheSchema );
        break;
      case "TWITTER":
        ParquetMetadata twitterMeta = ParquetFileReader
          .readFooter( conf, new Path( Paths.get( urlTestResources.toURI() ).toString() ),
            ParquetMetadataConverter.NO_FILTER );
        MessageType twitterSchema = twitterMeta.getFileMetaData().getSchema();
        kettleSchema = org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.ParquetConverter
          .buildInputFields( twitterSchema );
        break;
      default:
        Assert.fail( "Invalid provider name used." );
    }

    String marshallKettleSchema = new ParquetInputFieldList( kettleSchema ).marshall();
    Assert.assertEquals( marshallKettleSchema, expectedKettleSchema );
  }
}
