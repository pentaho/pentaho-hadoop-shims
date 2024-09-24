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

package org.pentaho.di.job.entries.hadoopjobexecutor;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;

import org.junit.Test;

public class SecurityManagerStackTest {

  private static LogChannelInterface log = new LogChannel( SecurityManagerStackTest.class.getName() );

  @Test( expected = IllegalArgumentException.class )
  public void setSecurityManager_null() {
    SecurityManagerStack stack = new SecurityManagerStack();
    stack.setSecurityManager( null );
  }

  @Test( expected = IllegalArgumentException.class )
  public void removeSecurityManager_null() {
    SecurityManagerStack stack = new SecurityManagerStack();
    stack.removeSecurityManager( null );
  }

  @Test( expected = IllegalStateException.class )
  public void removeSecurityManager_when_empty() {
    SecurityManagerStack stack = new SecurityManagerStack();
    stack.removeSecurityManager( new NoExitSecurityManager( null ) );
  }

  @Test
  public void simple_replace_in_isolation() {
    SecurityManager original = System.getSecurityManager();
    NoExitSecurityManager sm = new NoExitSecurityManager( null );

    SecurityManagerStack stack = new SecurityManagerStack();
    stack.setSecurityManager( sm );
    assertEquals( sm, System.getSecurityManager() );
    stack.removeSecurityManager( sm );
    assertEquals( original, System.getSecurityManager() );
  }

  @Test
  public void interweaved_executions() {
    SecurityManager original = System.getSecurityManager();
    NoExitSecurityManager sm1 = new NoExitSecurityManager( null );
    NoExitSecurityManager sm2 = new NoExitSecurityManager( null );

    SecurityManagerStack stack = new SecurityManagerStack();
    stack.setSecurityManager( sm1 );
    stack.setSecurityManager( sm2 );
    assertEquals( sm2, System.getSecurityManager() );
    stack.removeSecurityManager( sm1 );
    assertEquals( sm2, System.getSecurityManager() );
    stack.removeSecurityManager( sm2 );
    assertEquals( original, System.getSecurityManager() );
  }

  @Test
  public void sequential_executions() {
    SecurityManager original = System.getSecurityManager();
    NoExitSecurityManager sm1 = new NoExitSecurityManager( null );
    NoExitSecurityManager sm2 = new NoExitSecurityManager( null );

    SecurityManagerStack stack = new SecurityManagerStack();
    stack.setSecurityManager( sm1 );
    stack.setSecurityManager( sm2 );
    assertEquals( sm2, System.getSecurityManager() );
    stack.removeSecurityManager( sm2 );
    assertEquals( sm1, System.getSecurityManager() );
    stack.removeSecurityManager( sm1 );
    assertEquals( original, System.getSecurityManager() );
  }

  @Test
  public void randomized_executions() throws Exception {
    final SecurityManagerStack stack = new SecurityManagerStack();
    final Random random = new Random();

    // Set a flag security manager first that should still be set when all threads are complete
    NoExitSecurityManager test = new NoExitSecurityManager( null );
    SecurityManager original = System.getSecurityManager();
    stack.setSecurityManager( test );

    final int NUM_TASKS = 10;
    ExecutorService exec = Executors.newFixedThreadPool( NUM_TASKS );
    exec.invokeAll( Collections.nCopies( NUM_TASKS, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        NoExitSecurityManager sm = new NoExitSecurityManager( null );
        try {
          Thread.sleep( random.nextInt( 1000 ) );
          log.logBasic( "set: " + sm );
          stack.setSecurityManager( sm );
          Thread.sleep( random.nextInt( 1000 ) );
        } catch ( Exception ex ) {
          /* ignore */
        } finally {
          log.logBasic( "rm : \t" + sm );
          stack.removeSecurityManager( sm );
        }
        return null;
      }
    } ) );

    exec.shutdown();
    exec.awaitTermination( 3, TimeUnit.SECONDS );

    assertEquals( test, System.getSecurityManager() );
    stack.removeSecurityManager( test );
    assertEquals( original, System.getSecurityManager() );
  }
}
