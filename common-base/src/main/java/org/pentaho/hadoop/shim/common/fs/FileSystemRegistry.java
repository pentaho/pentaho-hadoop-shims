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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Static registry for Hadoop FileSystem implementations that works in non-OSGi environments
 * without relying on Java ServiceLoader or META-INF/services files.
 *
 * <p>This provides an alternative to ServiceLoader-based FileSystem registration by allowing
 * programmatic registration of FileSystem implementations that are applied to Hadoop
 * Configuration objects automatically.</p>
 *
 * <p><b>Usage:</b></p>
 * <pre>
 * // Register filesystems at application startup
 * FileSystemRegistry.registerFileSystem("pvfs", "org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge");
 * FileSystemRegistry.registerFileSystem("s3a", "org.apache.hadoop.fs.s3a.S3AFileSystem");
 * FileSystemRegistry.registerFileSystem("adl", "org.apache.hadoop.fs.adl.AdlFileSystem");
 *
 * // Apply to a Configuration
 * Configuration conf = new Configuration();
 * FileSystemRegistry.applyToConfiguration(conf);
 *
 * // Now FileSystem.get() will use registered implementations
 * FileSystem fs = FileSystem.get(uri, conf);
 * </pre>
 *
 * <p>This approach works in:</p>
 * <ul>
 *   <li>Standalone Hadoop applications</li>
 *   <li>Spark jobs</li>
 *   <li>MapReduce jobs</li>
 *   <li>Non-OSGi environments</li>
 *   <li>OSGi/Karaf environments (as alternative to ServiceLoader)</li>
 * </ul>
 */
public class FileSystemRegistry {

  private static final Logger LOGGER = LogManager.getLogger( FileSystemRegistry.class );

  // Registry of scheme -> implementation class mappings
  private static final Map<String, String> FILE_SYSTEM_IMPLEMENTATIONS = new HashMap<>();

  // Registry of scheme -> AbstractFileSystem implementation class mappings (Hadoop 2.x+)
  private static final Map<String, String> ABSTRACT_FILE_SYSTEM_IMPLEMENTATIONS = new HashMap<>();

  // Flag to track if default implementations have been registered
  private static boolean defaultsRegistered = false;

  static {
    // Auto-register default implementations on class load
    registerDefaults();
  }

  private FileSystemRegistry() {
    // Utility class, no instantiation
  }

  /**
   * Register default FileSystem implementations that ship with Pentaho.
   * This is called automatically on first use.
   */
  public static synchronized void registerDefaults() {
    if ( defaultsRegistered ) {
      return;
    }

    try {
      // PVFS - Pentaho Virtual File System bridge
      registerFileSystem( "pvfs", "org.pentaho.hadoop.shim.common.pvfs.PvfsHadoopBridge" );

      // Azure Data Lake Gen1
      registerFileSystem( "adl", "org.apache.hadoop.fs.adl.AdlFileSystem" );
      registerAbstractFileSystem( "adl", "org.apache.hadoop.fs.adl.Adl" );

      // Azure Blob Storage (Gen2)
      registerFileSystem( "wasb", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem" );
      registerFileSystem( "abfss", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem" );
      registerAbstractFileSystem( "abfss", "org.apache.hadoop.fs.azurebfs.Abfss" );

      // S3A (if available)
      registerFileSystemIfAvailable( "s3a", "org.apache.hadoop.fs.s3a.S3AFileSystem" );

      // Google Cloud Storage (if available)
      registerFileSystemIfAvailable( "gs", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" );
      registerAbstractFileSystemIfAvailable( "gs", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" );

      defaultsRegistered = true;
      LOGGER.info( "Registered {} default FileSystem implementations", FILE_SYSTEM_IMPLEMENTATIONS.size() );
    } catch ( Exception e ) {
      LOGGER.warn( "Error registering default FileSystem implementations", e );
    }
  }

  /**
   * Register a FileSystem implementation for a specific URI scheme.
   *
   * @param scheme              URI scheme (e.g., "s3a", "adl", "gs", "pvfs")
   * @param implementationClass Fully qualified class name of FileSystem implementation
   */
  public static synchronized void registerFileSystem( String scheme, String implementationClass ) {
    if ( scheme == null || implementationClass == null ) {
      throw new IllegalArgumentException( "Scheme and implementation class cannot be null" );
    }

    FILE_SYSTEM_IMPLEMENTATIONS.put( scheme, implementationClass );
    LOGGER.debug( "Registered FileSystem: {} -> {}", scheme, implementationClass );
  }

  /**
   * Register an AbstractFileSystem implementation for a specific URI scheme (Hadoop 2.x+).
   *
   * @param scheme              URI scheme (e.g., "s3a", "adl", "gs", "pvfs")
   * @param implementationClass Fully qualified class name of AbstractFileSystem implementation
   */
  public static synchronized void registerAbstractFileSystem( String scheme, String implementationClass ) {
    if ( scheme == null || implementationClass == null ) {
      throw new IllegalArgumentException( "Scheme and implementation class cannot be null" );
    }

    ABSTRACT_FILE_SYSTEM_IMPLEMENTATIONS.put( scheme, implementationClass );
    LOGGER.debug( "Registered AbstractFileSystem: {} -> {}", scheme, implementationClass );
  }

  /**
   * Register a FileSystem only if the implementation class is available on the classpath.
   * This prevents errors when optional dependencies are not present.
   *
   * @param scheme              URI scheme
   * @param implementationClass Fully qualified class name
   */
  public static synchronized void registerFileSystemIfAvailable( String scheme, String implementationClass ) {
    if ( isClassAvailable( implementationClass ) ) {
      registerFileSystem( scheme, implementationClass );
    } else {
      LOGGER.debug( "Skipping registration of {} - class not available: {}", scheme, implementationClass );
    }
  }

  /**
   * Register an AbstractFileSystem only if the implementation class is available on the classpath.
   *
   * @param scheme              URI scheme
   * @param implementationClass Fully qualified class name
   */
  public static synchronized void registerAbstractFileSystemIfAvailable( String scheme, String implementationClass ) {
    if ( isClassAvailable( implementationClass ) ) {
      registerAbstractFileSystem( scheme, implementationClass );
    } else {
      LOGGER.debug( "Skipping registration of AbstractFileSystem {} - class not available: {}", scheme, implementationClass );
    }
  }

  /**
   * Unregister a FileSystem implementation for a scheme.
   *
   * @param scheme URI scheme to unregister
   */
  public static synchronized void unregisterFileSystem( String scheme ) {
    FILE_SYSTEM_IMPLEMENTATIONS.remove( scheme );
    ABSTRACT_FILE_SYSTEM_IMPLEMENTATIONS.remove( scheme );
    LOGGER.debug( "Unregistered FileSystem: {}", scheme );
  }

  /**
   * Clear all registered FileSystem implementations.
   */
  public static synchronized void clearAll() {
    FILE_SYSTEM_IMPLEMENTATIONS.clear();
    ABSTRACT_FILE_SYSTEM_IMPLEMENTATIONS.clear();
    defaultsRegistered = false;
    LOGGER.debug( "Cleared all FileSystem registrations" );
  }

  /**
   * Apply all registered FileSystem implementations to a Hadoop Configuration.
   * This should be called before using FileSystem.get() to ensure custom
   * implementations are used.
   *
   * @param conf Hadoop Configuration object to configure
   */
  public static void applyToConfiguration( Configuration conf ) {
    if ( conf == null ) {
      throw new IllegalArgumentException( "Configuration cannot be null" );
    }

    // Apply FileSystem implementations
    for ( Map.Entry<String, String> entry : FILE_SYSTEM_IMPLEMENTATIONS.entrySet() ) {
      String key = "fs." + entry.getKey() + ".impl";
      conf.set( key, entry.getValue() );
      LOGGER.trace( "Applied to Configuration: {} = {}", key, entry.getValue() );
    }

    // Apply AbstractFileSystem implementations (Hadoop 2.x+)
    for ( Map.Entry<String, String> entry : ABSTRACT_FILE_SYSTEM_IMPLEMENTATIONS.entrySet() ) {
      String key = "fs.AbstractFileSystem." + entry.getKey() + ".impl";
      conf.set( key, entry.getValue() );
      LOGGER.trace( "Applied to Configuration: {} = {}", key, entry.getValue() );
    }

    if ( LOGGER.isDebugEnabled() ) {
      LOGGER.debug( "Applied {} FileSystem implementations to Configuration", FILE_SYSTEM_IMPLEMENTATIONS.size() );
    }
  }

  /**
   * Create a new Hadoop Configuration with all registered FileSystem implementations pre-applied.
   *
   * @return New Configuration object with FileSystem implementations configured
   */
  public static Configuration createConfiguration() {
    Configuration conf = new Configuration();
    applyToConfiguration( conf );
    return conf;
  }

  /**
   * Create a new Hadoop Configuration based on an existing one, with all registered
   * FileSystem implementations applied.
   *
   * @param baseConf Base configuration to copy settings from
   * @return New Configuration object with FileSystem implementations configured
   */
  public static Configuration createConfiguration( Configuration baseConf ) {
    Configuration conf = new Configuration( baseConf );
    applyToConfiguration( conf );
    return conf;
  }

  /**
   * Get the implementation class registered for a scheme.
   *
   * @param scheme URI scheme
   * @return Implementation class name, or null if not registered
   */
  public static String getImplementation( String scheme ) {
    return FILE_SYSTEM_IMPLEMENTATIONS.get( scheme );
  }

  /**
   * Check if a scheme has a registered implementation.
   *
   * @param scheme URI scheme
   * @return true if registered, false otherwise
   */
  public static boolean isRegistered( String scheme ) {
    return FILE_SYSTEM_IMPLEMENTATIONS.containsKey( scheme );
  }

  /**
   * Get all registered schemes.
   *
   * @return Map of scheme -> implementation class
   */
  public static Map<String, String> getAllRegistrations() {
    return new HashMap<>( FILE_SYSTEM_IMPLEMENTATIONS );
  }

  /**
   * Check if a class is available on the classpath.
   *
   * @param className Fully qualified class name
   * @return true if class is available, false otherwise
   */
  private static boolean isClassAvailable( String className ) {
    try {
      Class.forName( className, false, FileSystemRegistry.class.getClassLoader() );
      return true;
    } catch ( ClassNotFoundException e ) {
      return false;
    }
  }
}
