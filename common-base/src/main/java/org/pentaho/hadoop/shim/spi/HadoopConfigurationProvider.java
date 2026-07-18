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


package org.pentaho.hadoop.shim.spi;

import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;

import java.util.List;

/**
 * Provides a mechanism to load Hadoop configurations.
 *
 * @author Jordan Ganoff (jganoff@pentaho.com)
 */
public interface HadoopConfigurationProvider {

  /**
   * Query this provider to determine if it can provide a specific configuration.
   *
   * @param id Identifier of configuration to check for
   * @return {@code true} if the configuration can be obtained with this provider.
   */
  public boolean hasConfiguration( String id );

  /**
   * Retrieve all known configurations.
   *
   * @return List of all configurations available through this provider.
   */
  public List<? extends HadoopConfiguration> getConfigurations();

  /**
   * Retrieve a configuration by identifier.
   *
   * @param id Identifier of the configuration to retrieve
   * @return The Hadoop connection whose id matches the provided one.
   * @throws ConfigurationException Error retrieving the desired configuration
   */
  public HadoopConfiguration getConfiguration( String id ) throws ConfigurationException;

  /**
   * Retrieve the current "active" Hadoop configuration.
   *
   * @return The currently active Hadoop configuration
   * @throws ConfigurationException Error retrieving the active configuration
   */
  public HadoopConfiguration getActiveConfiguration() throws ConfigurationException;
}