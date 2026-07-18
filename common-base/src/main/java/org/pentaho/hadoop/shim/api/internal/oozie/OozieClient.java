/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2002 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/

package org.pentaho.hadoop.shim.api.internal.oozie;

import java.util.Properties;

public interface OozieClient {
  public String getClientBuildVersion();

  public boolean hasAppPath( Properties properties );

  public String getProtocolUrl() throws OozieClientException;

  public void validateWSVersion() throws OozieClientException;

  public OozieJob run( Properties conf ) throws OozieClientException;

  public OozieJob getJob( String jobId );
}