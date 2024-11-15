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


package org.pentaho.hadoop.shim.common.mapred;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class RunningJobProxyTest {

  @Test( expected = NullPointerException.class )
  public void instantiate_null_delegate() {
    new RunningJobProxy( null );
  }

  @Test
  public void isComplete() throws IOException {
    final AtomicBoolean called = new AtomicBoolean( false );
    RunningJobProxy proxy = new RunningJobProxy( new MockRunningJob() {
      @Override
      public boolean isComplete() throws IOException {
        called.set( true );
        return true;
      }
    } );

    assertTrue( proxy.isComplete() );
    assertTrue( called.get() );
  }

  @Test
  public void killJob() throws IOException {
    final AtomicBoolean called = new AtomicBoolean( false );
    RunningJobProxy proxy = new RunningJobProxy( new MockRunningJob() {
      @Override
      public void killJob() throws IOException {
        called.set( true );
      }
    } );

    proxy.killJob();

    assertTrue( called.get() );
  }

}
