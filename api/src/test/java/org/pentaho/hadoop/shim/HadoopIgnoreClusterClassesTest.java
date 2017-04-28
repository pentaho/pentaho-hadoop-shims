/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.hadoop.shim;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HadoopIgnoreClusterClassesTest {

  private static String PMR_PROPERTIES = "pmr.properties";
  private static File pmrFolder;
  private static URL urlTestResources;


  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    pmrFolder = tempFolder.newFolder( "pmr" );
    urlTestResources = Thread.currentThread().getContextClassLoader().getResource( PMR_PROPERTIES );
    Files.copy( Paths.get( urlTestResources.toURI() ), Paths.get( pmrFolder.getAbsolutePath(), PMR_PROPERTIES ) );
  }

  private void activatePmrFile() throws URISyntaxException, IOException {
    if ( !Files.exists( Paths.get( urlTestResources.toURI() ) ) ) {
      Files.copy( Paths.get( pmrFolder.getAbsolutePath(), PMR_PROPERTIES ), Paths.get( urlTestResources.toURI() ) );
    }
  }

  private void disablePmrFile() throws URISyntaxException, IOException {
    Files.deleteIfExists( Paths.get( urlTestResources.toURI() ) );
  }

  @Test
  public void isIgnoreClusterClassesPropertyNotEmpty_isPmrFalse() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();

    try {
      disablePmrFile();
      assertFalse( locator.isIgnoreClusterClassesPropertyNotEmpty( null ) );
      assertFalse( locator.isIgnoreClusterClassesPropertyNotEmpty( "" ) );
      assertFalse( locator.isIgnoreClusterClassesPropertyNotEmpty( "hadoop-common" ) );
    } finally {
      activatePmrFile();
    }
  }

  @Test
  public void isIgnoreClusterClassesPropertyNotEmpty_isPmrTrue() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();

    activatePmrFile();

    assertFalse( locator.isIgnoreClusterClassesPropertyNotEmpty( null ) );
    assertFalse( locator.isIgnoreClusterClassesPropertyNotEmpty( "" ) );
    assertEquals( true, locator.isIgnoreClusterClassesPropertyNotEmpty( "hadoop-common" ) );
  }
}
