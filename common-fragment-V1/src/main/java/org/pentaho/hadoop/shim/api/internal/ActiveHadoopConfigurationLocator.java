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

package org.pentaho.hadoop.shim.api.internal;

import org.pentaho.hadoop.shim.api.ConfigurationException;

/**
 * Provides a mechanism for a {@link HadoopConfigurationProvider} to locate the active {@link HadoopConfiguration} by
 * id.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public interface ActiveHadoopConfigurationLocator {
  /**
   * @return the active Hadoop configuration's identifier
   * @throws ConfigurationException Error determining the currently active Hadoop configuration
   */
  String getActiveConfigurationId() throws ConfigurationException;
}
