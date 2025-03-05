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


package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/29/16.
 */
public class HBaseTableWriteOperationManagerImplTest {
  private HBaseConnectionHandle hBaseConnectionHandle;
  private boolean autoFlush;
  private HBaseTableWriteOperationManagerImpl hBaseTableWriteOperationManager;
  private HBaseConnectionWrapper hBaseConnectionWrapper;

  @Before
  public void setup() {
    hBaseConnectionHandle = mock( HBaseConnectionHandle.class );
    hBaseConnectionWrapper = mock( HBaseConnectionWrapper.class );
    when( hBaseConnectionHandle.getConnection() ).thenReturn( hBaseConnectionWrapper );
    autoFlush = true;
    init();
  }

  private void init() {
    hBaseTableWriteOperationManager = new HBaseTableWriteOperationManagerImpl( hBaseConnectionHandle, autoFlush );
  }

  @Test
  public void testIsAutoFlush() {
    autoFlush = true;
    init();
    assertTrue( hBaseTableWriteOperationManager.isAutoFlush() );
    autoFlush = false;
    init();
    assertFalse( hBaseTableWriteOperationManager.isAutoFlush() );
  }

  @Test
  public void testCreatePut() throws Exception {
    byte[] bytes = "putKey".getBytes( Charset.forName( "UTF-8" ) );
    hBaseTableWriteOperationManager.createPut( bytes ).execute();
    verify( hBaseConnectionWrapper ).newTargetTablePut( bytes, false );
    verify( hBaseConnectionWrapper ).executeTargetTablePut();
  }

  @Test
  public void testDelete() throws Exception {
    byte[] bytes = "deleteKey".getBytes( Charset.forName( "UTF-8" ) );
    hBaseTableWriteOperationManager.createDelete( bytes ).execute();
    verify( hBaseConnectionWrapper ).executeTargetTableDelete( bytes );
  }

  @Test
  public void testFlushCommitsSuccess() throws Exception {
    hBaseTableWriteOperationManager.flushCommits();
    verify( hBaseConnectionWrapper ).flushCommitsTargetTable();
  }

  @Test( expected = IOException.class )
  public void testFlushCommitsException() throws Exception {
    Exception exception = new Exception();
    doThrow( exception ).when( hBaseConnectionWrapper ).flushCommitsTargetTable();
    try {
      hBaseTableWriteOperationManager.flushCommits();
    } catch ( IOException e ) {
      assertEquals( exception, e.getCause() );
      throw e;
    }
  }

  @Test
  public void testClose() throws IOException {
    hBaseTableWriteOperationManager.close();
    verify( hBaseConnectionHandle ).close();
  }
}
