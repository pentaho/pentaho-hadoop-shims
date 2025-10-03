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

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.pentaho.hadoop.shim.api.internal.oozie.OozieClientException;
import org.pentaho.hadoop.shim.api.internal.oozie.OozieJob;

public class OozieJobInfoImpl implements OozieJob {
  private final String id;
  private final OozieClient oozieClient;

  public OozieJobInfoImpl( String id, OozieClient oozieClient ) {
    this.id = id;
    this.oozieClient = oozieClient;
  }

  @Override
  public boolean didSucceed() throws OozieClientException {
    try {
      return oozieClient.getJobInfo( id ).getStatus().equals( WorkflowJob.Status.SUCCEEDED );
    } catch ( org.apache.oozie.client.OozieClientException e ) {
      throw new OozieClientException( e, e.getErrorCode() );
    }
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getJobLog() throws OozieClientException {
    try {
      return oozieClient.getJobLog( id );
    } catch ( org.apache.oozie.client.OozieClientException e ) {
      throw new OozieClientException( e, e.getErrorCode() );
    }
  }

  @Override
  public boolean isRunning() throws OozieClientException {
    try {
      return oozieClient.getJobInfo( id ).getStatus().equals( WorkflowJob.Status.RUNNING );
    } catch ( org.apache.oozie.client.OozieClientException e ) {
      throw new OozieClientException( e, e.getErrorCode() );
    }
  }

}
