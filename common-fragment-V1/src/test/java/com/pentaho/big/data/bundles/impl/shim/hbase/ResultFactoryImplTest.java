/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.hbase.ResultFactoryException;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;

import java.nio.charset.Charset;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 2/2/16.
 */
public class ResultFactoryImplTest {

  private HBaseBytesUtilShim hBaseBytesUtilShim;
  private ResultFactoryImpl resultFactory;

  @Before
  public void setup() {
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    resultFactory = new ResultFactoryImpl( hBaseBytesUtilShim );
  }

  @Test
  public void testCanHandle() {
    assertTrue( resultFactory.canHandle( null ) );
    assertTrue( resultFactory.canHandle( mock( Result.class ) ) );
    assertFalse( resultFactory.canHandle( new Object() ) );
  }

  @Test
  public void testCreateNull() throws ResultFactoryException {
    assertNull( resultFactory.create( null ) );
  }

  @Test
  public void testCreateSuccess() throws ResultFactoryException {
    Result delegate = mock( Result.class );
    byte[] row = "row".getBytes( Charset.forName( "UTF-8" ) );
    when( delegate.getRow() ).thenReturn( row );

    ResultImpl result = resultFactory.create( delegate );
    assertArrayEquals( row, result.getRow() );
  }

  @Test( expected = ResultFactoryException.class )
  public void testCreateException() throws ResultFactoryException {
    resultFactory.create( new Object() );
  }
}
