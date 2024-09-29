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


package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/29/16.
 */
public class HBaseDeleteImplTest {
  private HBaseConnectionHandle hBaseConnectionHandle;
  private byte[] testKey;
  private HBaseDeleteImpl hBaseDelete;
  private HBaseConnectionWrapper hBaseConnectionWrapper;

  @Before
  public void setup() {
    hBaseConnectionHandle = mock( HBaseConnectionHandle.class );
    hBaseConnectionWrapper = mock( HBaseConnectionWrapper.class );
    when( hBaseConnectionHandle.getConnection() ).thenReturn( hBaseConnectionWrapper );
    testKey = "testKey".getBytes( Charset.forName( "UTF-8" ) );
    hBaseDelete = new HBaseDeleteImpl( hBaseConnectionHandle, testKey );
  }

  @Test
  public void testExecuteSuccess() throws Exception {
    hBaseDelete.execute();
    verify( hBaseConnectionWrapper ).executeTargetTableDelete( testKey );
  }

  @Test( expected = IOException.class )
  public void testExecuteException() throws Exception {
    Exception exception = new Exception();
    doThrow( exception ).when( hBaseConnectionWrapper ).executeTargetTableDelete( testKey );
    try {
      hBaseDelete.execute();
    } catch ( IOException e ) {
      assertEquals( exception, e.getCause() );
      throw e;
    }
  }
}
