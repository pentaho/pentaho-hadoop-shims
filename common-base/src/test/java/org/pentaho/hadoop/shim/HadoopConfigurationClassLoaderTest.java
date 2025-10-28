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

import org.junit.Test;

import java.io.File;
import java.net.URL;

import static org.junit.Assert.*;

public class HadoopConfigurationClassLoaderTest {

  @Test( expected = NullPointerException.class )
  public void instantiation_null_URLs() {
    new HadoopConfigurationClassLoader( null, null );
  }

  @Test( expected = NullPointerException.class )
  public void instantiation_null_parent() {
    new HadoopConfigurationClassLoader( new URL[ 0 ], null );
  }

  @Test
  public void ignoreClass_built_ins() {
    HadoopConfigurationClassLoader hccl =
      new HadoopConfigurationClassLoader( new URL[ 0 ], getClass().getClassLoader() );
    assertTrue( hccl.ignoreClass( "org.apache.commons.log" ) );
    assertTrue( hccl.ignoreClass( "org.apache.logging.log4j" ) );
    assertTrue( hccl.ignoreClass( "org.apache.logging.log4j.Logger" ) );
    assertFalse( hccl.ignoreClass( "bogus" ) );
    assertTrue( hccl.ignoreClass( null ) );
  }

  @Test
  public void generateClassPahtString_single() throws Exception {
    URL workingDir = new File( "." ).toURI().toURL();
    HadoopConfigurationClassLoader hccl =
      new HadoopConfigurationClassLoader( new URL[] { workingDir }, getClass().getClassLoader() );
    assertEquals( workingDir.getFile(), hccl.generateClassPathString() );
  }

  @Test
  public void generateClassPahtString_multiple() throws Exception {
    URL workingDir = new File( "." ).toURI().toURL();
    URL srcDir = new File( "src" ).toURI().toURL();
    HadoopConfigurationClassLoader hccl =
      new HadoopConfigurationClassLoader( new URL[] { workingDir, srcDir }, getClass().getClassLoader() );
    assertEquals( workingDir.getFile() + File.pathSeparator + srcDir.getFile(), hccl.generateClassPathString() );
  }
}
