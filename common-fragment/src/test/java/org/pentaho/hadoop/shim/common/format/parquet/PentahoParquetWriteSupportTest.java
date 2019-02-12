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

import org.apache.hadoop.conf.Configuration;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.pentaho.di.core.util.Assert;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;


import java.util.Arrays;

@RunWith(Parameterized.class)
public class PentahoParquetWriteSupportTest {

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] { { "APACHE" }, { "TWITTER" } });
  }

  @Parameterized.Parameter
  public String provider;

  private static Configuration conf = new Configuration();
  static {
    conf.set( "fs.defaultFS", "file:///" );
  }

  @Test( expected = RuntimeException.class )
  public void initParquetWriteSupportWhenSchemaIsNull() {

    switch( provider ) {
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
        org.junit.Assert.fail("Invalid provider name used.");
    }
  }

  @Test
  public void initParquetWriteSupportWhenSchemaIsNotNull() {

    switch( provider ) {
      case "APACHE":
        org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoParquetWriteSupport apacheWriteSupport =
                new org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoParquetWriteSupport( ParquetUtils.createOutputFields( ParquetSpec.DataType.UTF8, false, ParquetSpec.DataType.INT_64, false ) );
        org.apache.parquet.hadoop.api.WriteSupport.WriteContext apacheWriteContext = apacheWriteSupport.init( conf );
        Assert.assertNotNull( apacheWriteContext );
        break;
      case "TWITTER":
        org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoParquetWriteSupport twitterWriteSupport =
                new org.pentaho.hadoop.shim.common.format.parquet.delegate.twitter.PentahoParquetWriteSupport( ParquetUtils.createOutputFields( ParquetSpec.DataType.UTF8, false, ParquetSpec.DataType.INT_64, false ) );
        parquet.hadoop.api.WriteSupport.WriteContext twitterWriteContext = twitterWriteSupport.init( conf );
        Assert.assertNotNull( twitterWriteContext );
        break;
      default:
        org.junit.Assert.fail("Invalid provider name used.");
    }
  }
}
