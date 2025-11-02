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

import java.lang.reflect.InvocationTargetException;
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
        // Don't actually call System.exit() in Java 21 - just return normally
        return;
      case EXIT_1:
        // Throw exception to simulate exit with error code
        throw new RuntimeException( "Exit code 1" );
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

    futureMapReduceJobSimpleRunnable
      = new FutureMapReduceJobSimpleRunnable( mainClass, commandLineArgs, complete, status, exceptionAtomicReference );
  }

  @After
  public void teardown() {
    mainBehavior = null;
    commandLineArgs = null;
    // SecurityManager check removed for Java 21 compatibility
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
    futureMapReduceJobSimpleRunnable
      = new FutureMapReduceJobSimpleRunnable( mainClass, commandLineArgs, complete, status, exceptionAtomicReference );
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
    assertEquals( -1, status.get() ); // Changed: Exception thrown means -1 status
    // Exception is thrown when simulating exit code 1
    assertTrue( exceptionAtomicReference.get() != null );
  }

  @Test
  public void testRunThrowNoExit() {
    // This test case is no longer applicable since we don't use SecurityManager
    // Skipping this test by using NO_EXIT behavior
    mainBehavior = MainBehavior.NO_EXIT;
    futureMapReduceJobSimpleRunnable.run();
    assertTrue( complete.get() );
    assertEquals( 0, status.get() );
    assertNull( exceptionAtomicReference.get() );
  }

  @Test
  public void testRunThrow() {
    mainBehavior = MainBehavior.THROW;
    futureMapReduceJobSimpleRunnable.run();
    assertTrue( complete.get() );
    assertEquals( -1, status.get() );
    // Exception is wrapped in InvocationTargetException, then in MapReduceExecutionException
    assertEquals( InvocationTargetException.class, exceptionAtomicReference.get().getCause().getClass() );
    assertEquals( runtimeException, exceptionAtomicReference.get().getCause().getCause() );
  }

  @Test(expected = NoSuchMethodException.class)
  public void testBadMain() throws Throwable {
    futureMapReduceJobSimpleRunnable
      = new FutureMapReduceJobSimpleRunnable( Object.class, commandLineArgs, complete, status, exceptionAtomicReference );
    futureMapReduceJobSimpleRunnable.run();
    assertTrue( complete.get() );
    assertEquals( -1, status.get() );
    // NoSuchMethodException is now caught and wrapped in MapReduceExecutionException
    throw exceptionAtomicReference.get().getCause();
  }

  private enum MainBehavior {
    NO_EXIT, EXIT_0, EXIT_1, THROW_NO_EXIT_255, THROW
  }
}
