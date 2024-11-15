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


package org.pentaho.hadoop.shim.api.internal.oozie;


public interface OozieJob {
  public String getId();

  public boolean isRunning() throws OozieClientException;

  public boolean didSucceed() throws OozieClientException;

  public String getJobLog() throws OozieClientException;
}
