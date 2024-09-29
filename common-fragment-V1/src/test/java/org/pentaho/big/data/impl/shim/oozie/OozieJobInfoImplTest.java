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


package org.pentaho.big.data.impl.shim.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class OozieJobInfoImplTest {

  @Mock OozieClient client;
  @Mock WorkflowJob workflowJob;
  @Mock org.apache.oozie.client.OozieClientException exception;
  String id = "ID";
  OozieJobInfoImpl oozieJobInfo;

  @Before
  public void before() throws OozieClientException {
    oozieJobInfo = new OozieJobInfoImpl( id, client );
    when( client.getJobInfo( id ) ).thenReturn( workflowJob );
  }

  @Test
  public void testDidSucceed() throws Exception {
    when( workflowJob.getStatus() ).thenReturn( WorkflowJob.Status.SUCCEEDED );
    assertTrue( oozieJobInfo.didSucceed() );
  }

  @Test
  public void testDidntSucceed() throws Exception {
    when( workflowJob.getStatus() ).thenReturn( WorkflowJob.Status.FAILED );
    assertFalse( oozieJobInfo.didSucceed() );
  }

  @Test
  public void testGetId() throws Exception {
    assertThat( oozieJobInfo.getId(), is( id ) );
  }

  @Test
  public void testGetJobLog() throws Exception {
    when( client.getJobLog( id ) ).thenReturn( "JOB LOG" );
    assertThat( oozieJobInfo.getJobLog(),
      is( "JOB LOG" ) );
  }

  @Test
  public void testClientThrows() throws OozieClientException {
    when( client.getJobInfo( id ) ).thenThrow( exception );
    when( client.getJobLog( id ) ).thenThrow( exception );
    try {
      oozieJobInfo.didSucceed();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( org.pentaho.hadoop.shim.api.internal.oozie.OozieClientException.class ) );
    }
    try {
      oozieJobInfo.getJobLog();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( org.pentaho.hadoop.shim.api.internal.oozie.OozieClientException.class ) );
    }
    try {
      oozieJobInfo.isRunning();
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( org.pentaho.hadoop.shim.api.internal.oozie.OozieClientException.class ) );
    }
  }

  @Test
  public void testIsRunning() throws Exception {
    when( workflowJob.getStatus() ).thenReturn( WorkflowJob.Status.RUNNING );
    assertThat( oozieJobInfo.isRunning(), is( true ) );

  }
}
