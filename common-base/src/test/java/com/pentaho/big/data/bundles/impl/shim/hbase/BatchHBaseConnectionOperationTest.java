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


package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/2/16.
 */
public class BatchHBaseConnectionOperationTest {
  @Test
  public void testBatchHBaseConnectionOperationTest() throws IOException {
    final HBaseConnectionWrapper hBaseConnectionWrapper = mock( HBaseConnectionWrapper.class );
    BatchHBaseConnectionOperation batchHBaseConnectionOperation = new BatchHBaseConnectionOperation();
    HBaseConnectionOperation operation1 = mock( HBaseConnectionOperation.class );
    final HBaseConnectionOperation operation2 = mock( HBaseConnectionOperation.class );
    batchHBaseConnectionOperation.addOperation( operation1 );
    batchHBaseConnectionOperation.addOperation( operation2 );

    doAnswer( new Answer<Void>() {
      @Override public Void answer( InvocationOnMock invocation ) throws Throwable {
        verify( operation2, never() ).perform( hBaseConnectionWrapper );
        return null;
      }
    } ).when( operation1 ).perform( hBaseConnectionWrapper );

    batchHBaseConnectionOperation.perform( hBaseConnectionWrapper );

    verify( operation1 ).perform( hBaseConnectionWrapper );
    verify( operation2 ).perform( hBaseConnectionWrapper );
  }
}
