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

import static org.junit.Assert.*;

import org.junit.Test;

public class ShimVersionTest {

  @Test( expected = IllegalArgumentException.class )
  public void instantation_invalid_version_major() {
    new ShimVersion( -1, 0 );
  }

  @Test( expected = IllegalArgumentException.class )
  public void instantation_invalid_version_minor() {
    new ShimVersion( 0, -1 );
  }

  @Test( expected = IllegalArgumentException.class )
  public void instantation_invalid_version_micro() {
    new ShimVersion( 0, 0, -1 );
  }

  @Test
  public void getMajorVersion() {
    ShimVersion version = new ShimVersion( 1, 0 );

    assertEquals( 1, version.getMajorVersion() );
  }

  @Test
  public void getMinorVersion() {
    ShimVersion version = new ShimVersion( 1, 1 );

    assertEquals( 1, version.getMinorVersion() );
  }

  @Test
  public void getMicroVersion() {
    ShimVersion version = new ShimVersion( 1, 1 );
    // Test Default
    assertEquals( 0, version.getMicroVersion() );

    version = new ShimVersion( 1, 1, 1 );
    assertEquals( 1, version.getMicroVersion() );
  }

  @Test
  public void getQualifierVersion() {
    String qualifier = "SNAPSHOT";
    ShimVersion version = new ShimVersion( 0, 0, 0, qualifier );

    assertEquals( qualifier, version.getQualifierVersion() );
  }

  @Test
  public void getVersion() {
    ShimVersion version = new ShimVersion( 1, 2, 3, "four" );

    assertEquals( "1.2.3.four", version.getVersion() );
  }

  @Test
  public void getVersion_no_qualifier() {
    ShimVersion version = new ShimVersion( 1, 2, 3 );

    assertEquals( "1.2.3", version.getVersion() );
  }

  @Test
  public void testToString() {
    ShimVersion version = new ShimVersion( 4, 3, 2, "one" );

    assertEquals( "4.3.2.one", version.toString() );
  }
}
