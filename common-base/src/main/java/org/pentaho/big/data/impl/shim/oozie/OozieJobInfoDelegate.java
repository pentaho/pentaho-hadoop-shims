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


import org.pentaho.hadoop.shim.api.internal.oozie.OozieClientException;
import org.pentaho.hadoop.shim.api.internal.oozie.OozieJob;
import org.pentaho.hadoop.shim.api.oozie.OozieJobInfo;
import org.pentaho.hadoop.shim.api.oozie.OozieServiceException;

public class OozieJobInfoDelegate implements OozieJobInfo {
  private final OozieJob oozieJob;

  public OozieJobInfoDelegate( OozieJob oozieJob ) {
    this.oozieJob = oozieJob;
  }

  @Override
  public boolean didSucceed() throws OozieServiceException {
    try {
      return oozieJob.didSucceed();
    } catch ( OozieClientException e ) {
      throw new OozieServiceException( e.getCause(), e.getErrorCode() );
    }
  }

  @Override
  public String getId() {
    return oozieJob.getId();
  }

  @Override
  public String getJobLog() throws OozieServiceException {
    try {
      return oozieJob.getJobLog();
    } catch ( OozieClientException e ) {
      throw new OozieServiceException( e.getCause(), e.getErrorCode() );
    }

  }

  @Override
  public boolean isRunning() throws OozieServiceException {
    try {
      return oozieJob.isRunning();
    } catch ( OozieClientException e ) {
      throw new OozieServiceException( e.getCause(), e.getErrorCode() );
    }
  }

}
