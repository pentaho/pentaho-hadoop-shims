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


package org.pentaho.di.job.entries.hadoopjobexecutor;

import static org.junit.Assert.*;

import org.junit.Test;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entries.hadoopjobexecutor.NoExitSecurityManager.NoExitSecurityException;

public class NoExitSecurityManagerTest {

  @Test
  public void checkExit_blocked_thread() {
    NoExitSecurityManager nesm = new NoExitSecurityManager( System.getSecurityManager() );
    nesm.addBlockedThread( Thread.currentThread() );
    int status = 1;
    try {
      nesm.checkExit( status );
      fail( "expected exception" );
    } catch ( NoExitSecurityException ex ) {
      assertEquals( status, ex.getStatus() );
      assertEquals( BaseMessages.getString( NoExitSecurityManager.class, "NoSystemExit" ), ex.getMessage() );
    }
  }

  @Test
  public void checkExit_nonblocked_thread() {
    NoExitSecurityManager nesm = new NoExitSecurityManager( System.getSecurityManager() );
    try {
      nesm.checkExit( 1 );
    } catch ( NoExitSecurityException ex ) {
      fail( "Should have been able to exit" );
    }
  }
}
