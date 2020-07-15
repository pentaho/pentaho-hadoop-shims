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
package org.pentaho.hadoop.shim.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.pentaho.di.core.vfs.KettleVFS;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;

public class DistributedCacheTestUtil {

  private static LogChannelInterface log = new LogChannel( DistributedCacheTestUtil.class.getName() );

  static FileObject createTestFolderWithContent() throws Exception {
    return createTestFolderWithContent( "sample-folder" );
  }

  static FileObject createTestFolderWithContent( String rootFolderName ) throws Exception {
    return createTestFolderWithContent( rootFolderName, 2 );
  }

  static FileObject createTestFolderWithContent( String rootFolderName, int numJars ) throws Exception {
    String rootName = "bin/test/" + rootFolderName;
    FileObject root = KettleVFS.getFileObject( rootName );
    for ( int i = 1; i < numJars + 1; i++ ) {
      String jarName = "jar" + i + ".jar";
      root.resolveFile( jarName ).createFile();
    }
    root.resolveFile( "folder" ).resolveFile( "file.txt" ).createFile();
    root.resolveFile( "pentaho-mapreduce-libraries.zip" ).createFile();
    root.resolveFile( "system/karaf/system" ).createFolder();

    createTestHadoopConfiguration( rootName );

    return root;
  }

  static FileObject createTestHadoopConfiguration( String rootFolderName ) throws Exception {
    FileObject location = KettleVFS.getFileObject( rootFolderName + "/hadoop-configurations/test-config" );

    FileObject lib = location.resolveFile( "lib" );
    FileObject libPmr = lib.resolveFile( "pmr" );
    FileObject pmrLibJar = libPmr.resolveFile( "configuration-specific.jar" );

    lib.createFolder();
    lib.resolveFile( "required.jar" ).createFile();

    libPmr.createFolder();
    pmrLibJar.createFile();

    return location;
  }

  static FileSystem getLocalFileSystem( Configuration conf ) throws IOException {
    FileSystem fs = org.apache.hadoop.fs.FileSystem.getLocal( conf );
    try {
      Method setWriteChecksum = fs.getClass().getMethod( "setWriteChecksum", boolean.class );
      setWriteChecksum.invoke( fs, false );
    } catch ( Exception ex ) {
      // ignore, this Hadoop implementation doesn't support checksum verification
    }
    return fs;
  }

  /**
   * Utility to attempt to stage a file to HDFS for use with Distributed Cache.
   *
   * @param ch                Distributed Cache Helper
   * @param source            File or directory to stage
   * @param fs                FileSystem to stage to
   * @param root              Root directory to clean up when this test is complete
   * @param dest              Destination path to stage to
   * @param expectedFileCount Expected number of files to exist in the destination once staged
   * @param expectedDirCount  Expected number of directories to exist in the destiation once staged
   * @throws Exception
   */
  static void stageForCacheTester( DistributedCacheUtilImpl ch, FileObject source, FileSystem fs, Path root, Path dest,
                                   int expectedFileCount, int expectedDirCount ) throws Exception {
    try {
      ch.stageForCache( source, fs, dest, true );

      assertTrue( fs.exists( dest ) );
      ContentSummary cs = fs.getContentSummary( dest );
      assertEquals( expectedFileCount, cs.getFileCount() );
      assertEquals( expectedDirCount, cs.getDirectoryCount() );
      assertEquals( FsPermission.createImmutable( (short) 0755 ), fs.getFileStatus( dest ).getPermission() );
    } finally {
      // Clean up after ourself
      if ( !fs.delete( root, true ) ) {
        log.logError( "error deleting FileSystem temp dir " + root );
      }
    }
  }

}
