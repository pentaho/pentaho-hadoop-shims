/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
