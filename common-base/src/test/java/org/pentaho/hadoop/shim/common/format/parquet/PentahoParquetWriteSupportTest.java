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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.pentaho.di.core.util.Assert;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;


import java.util.Arrays;

@RunWith( Parameterized.class )
public class PentahoParquetWriteSupportTest {

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList( new Object[][] { { "APACHE" }, { "TWITTER" } } );
  }

  @Parameterized.Parameter
  public String provider;

  private static Configuration conf = new Configuration();

  static {
    conf.set( "fs.defaultFS", "file:///" );
  }

  @Test( expected = RuntimeException.class )
  public void initParquetWriteSupportWhenSchemaIsNull() {

    switch ( provider ) {
      case "APACHE":
        org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoParquetWriteSupport apacheWriteSupport =
          new org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoParquetWriteSupport( null );
        apacheWriteSupport.init( conf );
        break;
      case "TWITTER":
        org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoParquetWriteSupport twitterWriteSupport =
          new org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoParquetWriteSupport( null );
        twitterWriteSupport.init( conf );
        break;
      default:
        org.junit.Assert.fail( "Invalid provider name used." );
    }
  }

  @Test
  public void initParquetWriteSupportWhenSchemaIsNotNull() {

    switch ( provider ) {
      case "APACHE":
        org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoParquetWriteSupport apacheWriteSupport =
          new org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoParquetWriteSupport(
            ParquetUtils.createOutputFields( ParquetSpec.DataType.UTF8, false, ParquetSpec.DataType.INT_64, false ) );
        org.apache.parquet.hadoop.api.WriteSupport.WriteContext apacheWriteContext = apacheWriteSupport.init( conf );
        Assert.assertNotNull( apacheWriteContext );
        break;
      case "TWITTER":
        org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoParquetWriteSupport twitterWriteSupport =
          new org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoParquetWriteSupport(
            ParquetUtils.createOutputFields( ParquetSpec.DataType.UTF8, false, ParquetSpec.DataType.INT_64, false ) );
        WriteSupport.WriteContext twitterWriteContext = twitterWriteSupport.init( conf );
        Assert.assertNotNull( twitterWriteContext );
        break;
      default:
        org.junit.Assert.fail( "Invalid provider name used." );
    }
  }
}
