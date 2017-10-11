/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import org.junit.Test;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;
import org.pentaho.hadoop.shim.spi.SnappyShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.pentaho.hbase.shim.spi.HBaseShim;

import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Validate that our Shim service providers have been registered properly
 */
public class ShimRegistrationTest {

  /**
   * Make sure we've registered our Hadoop Shim
   */
  @Test
  public void hadoopShimRegistered() {
    assertRegistered( HadoopShim.class );
  }

  /**
   * Make sure we've registered our Pig Shim
   */
  @Test
  public void pigShimRegistered() {
    assertRegistered( PigShim.class );
  }

  /**
   * Make sure we've registered our Pig Shim
   */
  @Test
  public void sqoopShimRegistered() {
    assertRegistered( SqoopShim.class );
  }

  /**
   * Make sure we've registered our Snappy Shim
   */
  @Test
  public void snappyShimRegistered() {
    assertRegistered( SnappyShim.class );
  }

  /**
   * Make sure we've registered our HBase Shim
   */
  @Test
  public void hbaseShimRegistered() {
    assertRegistered( HBaseShim.class );
  }

  private <T> void assertRegistered( Class<T> type ) {
    try {
      ServiceLoader<T> loader = ServiceLoader.load( type );
      T shim = loader.iterator().next();
      assertTrue( shim != null );
    } catch ( ServiceConfigurationError error ) {
      fail( error.getMessage() );
    }
  }
}
