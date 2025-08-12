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


package org.pentaho.big.data.impl.shim.oozie;

import org.apache.oozie.client.OozieClientException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.hadoop.shim.api.internal.oozie.OozieJob;
import org.pentaho.hadoop.shim.api.oozie.OozieServiceException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OozieJobInfoDelegateTest {

  @Mock
  OozieJob job;
  @Mock
  org.pentaho.hadoop.shim.api.internal.oozie.OozieClientException exception;
  String id = "ID";
  OozieJobInfoDelegate oozieJobInfoDelegate;

  @Before
  public void before() throws OozieClientException {
    oozieJobInfoDelegate = new OozieJobInfoDelegate( job );
  }

  @Test
  public void testDidSucceed() throws Exception {
    when( job.didSucceed() ).thenReturn( true );
    assertTrue( oozieJobInfoDelegate.didSucceed() );
  }

  @Test
  public void testDidntSucceed() throws Exception {
    when( job.didSucceed() ).thenReturn( false );
    assertFalse( oozieJobInfoDelegate.didSucceed() );
  }

  @Test
  public void testGetId() throws Exception {
    when( job.getId() ).thenReturn( id );
    assertThat( oozieJobInfoDelegate.getId(), is( id ) );
  }

  @Test
  public void testGetJobLog() throws Exception {
    when( job.getJobLog() ).thenReturn( "JOB LOG" );
    assertThat( oozieJobInfoDelegate.getJobLog(),
      is( "JOB LOG" ) );
  }

  @Test
  public void testClientThrows() throws org.pentaho.hadoop.shim.api.internal.oozie.OozieClientException {
    when( job.didSucceed() ).thenThrow( exception );
    when( job.isRunning() ).thenThrow( exception );
    when( job.getJobLog() ).thenThrow( exception );
    try {
      oozieJobInfoDelegate.didSucceed();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( OozieServiceException.class ) );
    }
    try {
      oozieJobInfoDelegate.getJobLog();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( OozieServiceException.class ) );
    }
    try {
      oozieJobInfoDelegate.isRunning();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( OozieServiceException.class ) );
    }
  }

  @Test
  public void testIsRunning() throws Exception {
    when( job.isRunning() ).thenReturn( true );
    assertThat( oozieJobInfoDelegate.isRunning(), is( true ) );

  }
}
