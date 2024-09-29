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


package org.pentaho.hadoop.shim;

import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.internal.ActiveHadoopConfigurationLocator;

public class MockActiveHadoopConfigurationLocator implements ActiveHadoopConfigurationLocator {

  private String activeId;

  public MockActiveHadoopConfigurationLocator() {
    this( null );
  }

  public MockActiveHadoopConfigurationLocator( String activeId ) {
    this.activeId = activeId;
  }

  @Override
  public String getActiveConfigurationId() throws ConfigurationException {
    return activeId;
  }

}
