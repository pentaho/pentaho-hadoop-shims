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


package org.pentaho.big.data.impl.shim.mapreduce;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.mapreduce.MapReduceExecutionException;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 12/7/15.
 */
public class FutureMapReduceJobSimpleRunnableTest {
  private static final RuntimeException runtimeException = new RuntimeException();
  private static final SecurityManager initialSecurityManager = System.getSecurityManager();
  private static MainBehavior mainBehavior;
  private static String commandLineArgs;
  private Class<? extends FutureMapReduceJobSimpleRunnableTest> mainClass;
  private AtomicBoolean complete;
  private AtomicInteger status;
  private AtomicReference<MapReduceExecutionException> exceptionAtomicReference;
  private FutureMapReduceJobSimpleRunnable futureMapReduceJobSimpleRunnable;

  public static void main( String[] args ) {
    if ( commandLineArgs == null ) {
      assertEquals( 0, args.length );
    } else {
      assertArrayEquals( commandLineArgs.split( " " ), args );
    }
    switch ( mainBehavior ) {
      case NO_EXIT:
        return;
      case EXIT_0:
        System.exit( 0 );
        break;
      case EXIT_1:
        System.exit( 1 );
        break;
      case THROW:
        throw runtimeException;
      default:
        throw new RuntimeException( "Unset mainBehavior" );
    }
  }

  @Before
  public void setup() {
    mainClass = getClass();
    commandLineArgs = "cli args";
    complete = new AtomicBoolean( false );
    status = new AtomicInteger( -1 );
    exceptionAtomicReference = new AtomicReference<>( null );

    futureMapReduceJobSimpleRunnable =
      new FutureMapReduceJobSimpleRunnable( mainClass, commandLineArgs, complete, status, exceptionAtomicReference );
  }

  @After
  public void teardown() {
    mainBehavior = null;
    commandLineArgs = null;
    assertEquals( initialSecurityManager, System.getSecurityManager() );
  }

  @Test
  public void testGetComplete() {
    assertEquals( complete, futureMapReduceJobSimpleRunnable.getComplete() );
  }

  @Test
  public void testGetStatus() {
    assertEquals( status, futureMapReduceJobSimpleRunnable.getStatus() );
  }

  @Test
  public void testGetExceptionAtomicReference() {
    assertEquals( exceptionAtomicReference, futureMapReduceJobSimpleRunnable.getExceptionAtomicReference() );
  }

  @Test
  public void testRunNoExit() {
    mainBehavior = MainBehavior.NO_EXIT;
    futureMapReduceJobSimpleRunnable.run();
    assertTrue( complete.get() );
    assertEquals( 0, status.get() );
    assertNull( exceptionAtomicReference.get() );
  }

  @Test
  public void testRunNoArgsNoExit() {
    commandLineArgs = null;
    futureMapReduceJobSimpleRunnable =
      new FutureMapReduceJobSimpleRunnable( mainClass, commandLineArgs, complete, status, exceptionAtomicReference );
    mainBehavior = MainBehavior.NO_EXIT;
    futureMapReduceJobSimpleRunnable.run();
    assertTrue( complete.get() );
    assertEquals( 0, status.get() );
    assertNull( exceptionAtomicReference.get() );
  }

  @Test
  public void testRunExit0() {
    mainBehavior = MainBehavior.EXIT_0;
    futureMapReduceJobSimpleRunnable.run();
    assertTrue( complete.get() );
    assertEquals( 0, status.get() );
    assertNull( exceptionAtomicReference.get() );
  }

  @Test
  public void testRunExit1() {
    mainBehavior = MainBehavior.EXIT_1;
    futureMapReduceJobSimpleRunnable.run();
    assertTrue( complete.get() );
    assertEquals( 1, status.get() );
    assertNull( exceptionAtomicReference.get() );
  }

  @Test
  public void testRunThrowNoExit() {
    mainBehavior = MainBehavior.THROW_NO_EXIT_255;
    futureMapReduceJobSimpleRunnable.run();
    assertTrue( complete.get() );
    assertEquals( 255, status.get() );
    assertNull( exceptionAtomicReference.get() );
  }

  @Test
  public void testRunThrow() {
    mainBehavior = MainBehavior.THROW;
    futureMapReduceJobSimpleRunnable.run();
    assertTrue( complete.get() );
    assertEquals( -1, status.get() );
    assertEquals( runtimeException, exceptionAtomicReference.get().getCause() );
  }

  @Test( expected = NoSuchMethodException.class )
  public void testBadMain() throws Throwable {
    futureMapReduceJobSimpleRunnable =
      new FutureMapReduceJobSimpleRunnable( Object.class, commandLineArgs, complete, status, exceptionAtomicReference );
    futureMapReduceJobSimpleRunnable.run();
    assertTrue( complete.get() );
    assertEquals( -1, status.get() );
    throw exceptionAtomicReference.get().getCause();
  }

  private enum MainBehavior {
    NO_EXIT, EXIT_0, EXIT_1, THROW_NO_EXIT_255, THROW
  }
}
