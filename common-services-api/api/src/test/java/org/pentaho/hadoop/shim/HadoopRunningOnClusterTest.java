/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.commons.vfs2.AllFileSelector;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.VFS;
import org.junit.Assert;
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
import java.util.List;

public class HadoopRunningOnClusterTest {

  private static String CONFIG_PROPERTY_CLASSPATH =
    System.getProperty( "java.io.tmpdir" ) + "/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/mapreduce";
  private int count;
  private static String PMR_PROPERTIES = "pmr.properties";
  private static File pmrFolder;
  private static URL urlTestResources;


  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws Exception {
    // Create a test hadoop configuration
    FileObject ramRoot = VFS.getManager().resolveFile( CONFIG_PROPERTY_CLASSPATH );
    if ( ramRoot.exists() ) {
      ramRoot.delete( new AllFileSelector() );
    }
    ramRoot.createFolder();

    // Create the implementation jars
    ramRoot.resolveFile( "hadoop-mapreduce-client-app-2.7.0-mapr-1602.jar" ).createFile();
    ramRoot.resolveFile( "hadoop-mapreduce-client-common-2.7.0-mapr-1602.jar" ).createFile();
    ramRoot.resolveFile( "hadoop-mapreduce-client-contrib-2.7.0-mapr-1602.jar" ).createFile();
    ramRoot.resolveFile( "hadoop-mapreduce-client-core-2.7.0-mapr-1602.jar" ).createFile();
    ramRoot.resolveFile( "hadoop-mapreduce-client-hs-2.7.0-mapr-1602.jar" ).createFile();

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
  public void isRunningOnCluster_PmrFalse() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();

    try {
      disablePmrFile();
      Assert.assertEquals( false, locator.isRunningOnCluster() );
    } finally {
      activatePmrFile();
    }
  }

  @Test
  public void isRunningOnCluster_PmrTrue() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();

    activatePmrFile();
    Assert.assertEquals( true, locator.isRunningOnCluster() );
  }


  @Test
  public void runningLocally_withLinuxClassPathProperty() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    FileObject folder = VFS.getManager().resolveFile( CONFIG_PROPERTY_CLASSPATH );

    try {
      disablePmrFile();
      List<URL> classpathElements = null;
      if ( !locator.isRunningOnCluster() ) {
        classpathElements = locator.parseURLs( folder, folder.toString() );
        count = classpathElements.size();
      }
      Assert.assertNotNull( classpathElements );
      Assert.assertEquals( 6, count );
    } finally {
      activatePmrFile();
    }
  }

  @Test
  public void runningOnCluster_ignoreLinuxClassPathProperty() throws Exception {
    HadoopConfigurationLocator locator = new HadoopConfigurationLocator();
    FileObject folder = VFS.getManager().resolveFile( CONFIG_PROPERTY_CLASSPATH );

    activatePmrFile();
    List<URL> classpathElements = null;
    if ( !locator.isRunningOnCluster() ) {
      classpathElements = locator.parseURLs( folder, folder.toString() );
      count = classpathElements.size();
    }
    Assert.assertEquals( null, classpathElements );
    Assert.assertEquals( 0, count );
  }
}
