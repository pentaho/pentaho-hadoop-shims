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

package org.pentaho.hadoop.shim.common.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;

/**
 * Utility class that wraps Hadoop FileSystem operations with automatic
 * FileSystemRegistry integration for non-OSGi environments.
 *
 * <p>This provides convenience methods that automatically apply registered
 * FileSystem implementations without requiring explicit Configuration setup.</p>
 *
 * <p><b>Simple Usage:</b></p>
 * <pre>
 * // Get a FileSystem with registered implementations automatically applied
 * FileSystem fs = FileSystemUtils.getFileSystem("pvfs://conn/bucket/file.txt");
 *
 * // Or with a specific Configuration
 * FileSystem fs = FileSystemUtils.getFileSystem(uri, myConfiguration);
 * </pre>
 */
public class FileSystemUtils {

  private static final Logger LOGGER = LogManager.getLogger( FileSystemUtils.class );

  // Ensure defaults are registered
  static {
    try {
      FileSystemRegistry.registerDefaults();
    } catch ( Exception e ) {
      LOGGER.warn( "Failed to register default FileSystem implementations", e );
    }
  }

  private FileSystemUtils() {
    // Utility class
  }

  /**
   * Get a FileSystem instance for the given URI with registered implementations applied.
   * Creates a new Configuration with all registered FileSystems.
   *
   * @param uri URI string (e.g., "pvfs://connection/bucket/file.txt")
   * @return FileSystem instance
   * @throws IOException if filesystem cannot be created
   */
  public static FileSystem getFileSystem( String uri ) throws IOException {
    return getFileSystem( URI.create( uri ) );
  }

  /**
   * Get a FileSystem instance for the given URI with registered implementations applied.
   * Creates a new Configuration with all registered FileSystems.
   *
   * @param uri URI object
   * @return FileSystem instance
   * @throws IOException if filesystem cannot be created
   */
  public static FileSystem getFileSystem( URI uri ) throws IOException {
    Configuration conf = FileSystemRegistry.createConfiguration();
    return FileSystem.get( uri, conf );
  }

  /**
   * Get a FileSystem instance with registered implementations applied to the provided Configuration.
   * This modifies the provided Configuration by adding FileSystem registrations.
   *
   * @param uri  URI string
   * @param conf Configuration to use (will be modified)
   * @return FileSystem instance
   * @throws IOException if filesystem cannot be created
   */
  public static FileSystem getFileSystem( String uri, Configuration conf ) throws IOException {
    return getFileSystem( URI.create( uri ), conf );
  }

  /**
   * Get a FileSystem instance with registered implementations applied to the provided Configuration.
   * This modifies the provided Configuration by adding FileSystem registrations.
   *
   * @param uri  URI object
   * @param conf Configuration to use (will be modified)
   * @return FileSystem instance
   * @throws IOException if filesystem cannot be created
   */
  public static FileSystem getFileSystem( URI uri, Configuration conf ) throws IOException {
    FileSystemRegistry.applyToConfiguration( conf );
    return FileSystem.get( uri, conf );
  }

  /**
   * Create a new Configuration with all registered FileSystem implementations.
   * This is equivalent to FileSystemRegistry.createConfiguration().
   *
   * @return New Configuration with FileSystem implementations registered
   */
  public static Configuration createConfiguration() {
    return FileSystemRegistry.createConfiguration();
  }

  /**
   * Create a new Configuration based on an existing one, with all registered
   * FileSystem implementations added.
   *
   * @param base Base configuration to copy from
   * @return New Configuration with base settings and FileSystem implementations
   */
  public static Configuration createConfiguration( Configuration base ) {
    return FileSystemRegistry.createConfiguration( base );
  }

  /**
   * Get the default FileSystem with registered implementations.
   * Uses the default URI from the configuration.
   *
   * @return Default FileSystem instance
   * @throws IOException if filesystem cannot be created
   */
  public static FileSystem getDefaultFileSystem() throws IOException {
    Configuration conf = FileSystemRegistry.createConfiguration();
    return FileSystem.get( conf );
  }

  /**
   * Get the local FileSystem with registered implementations.
   *
   * @return Local FileSystem instance
   * @throws IOException if filesystem cannot be created
   */
  public static FileSystem getLocalFileSystem() throws IOException {
    Configuration conf = FileSystemRegistry.createConfiguration();
    return FileSystem.getLocal( conf );
  }
}
