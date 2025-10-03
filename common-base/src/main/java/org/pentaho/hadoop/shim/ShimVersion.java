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


package org.pentaho.hadoop.shim;

/**
 * Represents the version of a Shim implementation. It follows the format: <p> {@code major.minor.micro.qualifier}.
 * </p>
 */
public class ShimVersion {

  private static final String DOT = ".";

  private int major;

  private int minor;

  private int micro;

  private String qualifier;

  public ShimVersion( int major, int minor ) {
    this( major, minor, 0 );
  }

  public ShimVersion( int major, int minor, int micro ) {
    this( major, minor, micro, null );
  }

  public ShimVersion( int major, int minor, int micro, String qualifier ) {
    if ( major < 0 || minor < 0 || micro < 0 ) {
      throw new IllegalArgumentException( "major, minor, and micro version numbers must be >= 0" );
    }
    this.major = major;
    this.minor = minor;
    this.micro = micro;
    this.qualifier = qualifier;
  }

  /**
   * @return the first version number, the "major" version
   */
  public int getMajorVersion() {
    return major;
  }

  /**
   * @return the second version number, the "minor" version
   */
  public int getMinorVersion() {
    return minor;
  }

  /**
   * @return the third version number, the "micro" version
   */
  public int getMicroVersion() {
    return micro;
  }

  /**
   * @return the fourth part of the version, the "qualifier" version string
   */
  public String getQualifierVersion() {
    return qualifier;
  }

  /**
   * Creates the {@code major.minor.micro[.qualifier]} string for this version
   */
  public String getVersion() {
    StringBuilder sb = new StringBuilder();
    sb.append( major );
    sb.append( DOT );
    sb.append( minor );
    sb.append( DOT );
    sb.append( micro );
    if ( qualifier != null ) {
      sb.append( DOT );
      sb.append( qualifier );
    }
    return sb.toString();
  }

  /**
   * @see #getVersion()
   */
  @Override
  public String toString() {
    return getVersion();
  }
}
