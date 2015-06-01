/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hbase.shim.mapr401;

import org.pentaho.hbase.shim.mapr401.wrapper.HBaseConnectionInterface;

import static org.junit.Assert.fail;

public class HBaseConnectionImplTest {

  private HBaseConnectionInterface hBaseConnection;
  private ClassLoader cl;

  @org.junit.Before
  public void setUp() throws Exception {
    cl = Thread.currentThread().getContextClassLoader();

    Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
    hBaseConnection = new MapRHBaseConnection();

  }

  @org.junit.After
  public void tearDown() throws Exception {
    Thread.currentThread().setContextClassLoader( cl );
  }

  @org.junit.Test
  public void testGetByteArrayComparableClass() throws Exception {
    try {
      hBaseConnection.getByteArrayComparableClass();
    } catch ( ClassNotFoundException e ) {
      e.printStackTrace();
      fail( "ByteArrayComparableClass is not accessible " );
    }
  }

  @org.junit.Test
  public void testGetCompressionAlgorithmClass() throws Exception {
    try {
      hBaseConnection.getCompressionAlgorithmClass();
    } catch ( ClassNotFoundException e ) {
      e.printStackTrace();
      fail( "CompressionAlgorithmClass is not accessible " );
    }
  }

  @org.junit.Test
  public void testGetBloomTypeClass() throws Exception {
    try {
      hBaseConnection.getBloomTypeClass();
    } catch ( ClassNotFoundException e ) {
      e.printStackTrace();
      fail( "BloomTypeClass is not accessible " );
    }
  }

  @org.junit.Test
  public void testGetDeserializedNumericComparatorClass() throws Exception {
    try {
      hBaseConnection.getDeserializedNumericComparatorClass();
    } catch ( ClassNotFoundException e ) {
      e.printStackTrace();
      fail( "DeserializedNumericComparatorClass is not accessible " );
    }
  }

  @org.junit.Test
  public void testGetDeserializedBooleanComparatorClass() throws Exception {
    try {
      hBaseConnection.getDeserializedBooleanComparatorClass();
    } catch ( ClassNotFoundException e ) {
      e.printStackTrace();
      fail( "DeserializedBooleanComparatorClass is not accessible " );
    }
  }
}
