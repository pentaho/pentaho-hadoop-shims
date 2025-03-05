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


package org.pentaho.hadoop.shim.api.oozie;

public interface OozieJobInfo {

  String getId();

  boolean isRunning() throws OozieServiceException;

  boolean didSucceed() throws OozieServiceException;

  String getJobLog() throws OozieServiceException;

}
