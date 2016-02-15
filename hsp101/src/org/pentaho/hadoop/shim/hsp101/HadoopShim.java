/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.hadoop.shim.hsp101;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationFileSystemManager;
import org.pentaho.hadoop.shim.common.DistributedCacheUtilImpl;
import org.pentaho.hadoop.shim.common.HadoopShimImpl;
import org.pentaho.hdfs.vfs.HDFSFileProvider;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class HadoopShim extends HadoopShimImpl {

  @Override
  protected String getDefaultJobtrackerPort() {
    return "50300";
  }

  @Override
  public void onLoad( HadoopConfiguration config, HadoopConfigurationFileSystemManager fsm ) throws Exception {
    fsm.addProvider( config, "hdfs", config.getIdentifier(), new HDFSFileProvider() );
    setDistributedCacheUtil( new DistributedCacheUtilImpl( config ) {
      /**
       * Default permission for cached files
       * <p/>
       * Not using FsPermission.createImmutable due to EOFExceptions when using it with Hadoop 0.20.2
       */
      private final FsPermission CACHED_FILE_PERMISSION = new FsPermission( (short) 0755 );

      public void addFileToClassPath( Path file, Configuration conf ) throws IOException {
        String classpath = conf.get( "mapred.job.classpath.files" );
        conf.set( "mapred.job.classpath.files",
          classpath == null ? file.toString() : classpath + getClusterPathSeparator() + file.toString() );
        FileSystem fs = FileSystem.get( conf );
        URI uri = fs.makeQualified( file ).toUri();

        DistributedCache.addCacheFile( uri, conf );
      }

      /**
       * Stages the source file or folder to a Hadoop file system and sets their permission and replication
       * value appropriately to be used with the Distributed Cache. WARNING: This will delete the contents of
       * dest before staging the archive.
       *
       * @param source    File or folder to copy to the file system. If it is a folder all contents will be
       *                  copied into dest.
       * @param fs        Hadoop file system to store the contents of the archive in
       * @param dest      Destination to copy source into. If source is a file, the new file name will be
       *                  exactly dest. If source is a folder its contents will be copied into dest. For more
       *                  info see {@link FileSystem#copyFromLocalFile(org.apache.hadoop.fs.Path,
       *                  org.apache.hadoop.fs.Path)}.
       * @param overwrite Should an existing file or folder be overwritten? If not an exception will be
       *                  thrown.
       * @throws IOException         Destination exists is not a directory
       * @throws KettleFileException Source does not exist or destination exists and overwrite is false.
       */
      public void stageForCache( FileObject source, FileSystem fs, Path dest, boolean overwrite )
        throws IOException, KettleFileException {
        if ( !source.exists() ) {
          throw new KettleFileException(
            BaseMessages
              .getString( DistributedCacheUtilImpl.class, "DistributedCacheUtil.SourceDoesNotExist", source ) );
        }

        if ( fs.exists( dest ) ) {
          if ( overwrite ) {
            // It is a directory, clear it out
            fs.delete( dest, true );
          } else {
            throw new KettleFileException( BaseMessages
              .getString( DistributedCacheUtilImpl.class, "DistributedCacheUtil.DestinationExists",
                dest.toUri().getPath() ) );
          }
        }

        // Use the same replication we'd use for submitting jobs
        short replication = (short) fs.getConf().getInt( "mapred.submit.replication", 10 );

        copyFile( source, fs, dest, overwrite );
        fs.setReplication( dest, replication );
      }

      private void copyFile( FileObject source, FileSystem fs, Path dest, boolean overwrite ) throws IOException {
        if ( source.getType() == FileType.FOLDER ) {
          fs.mkdirs( dest );
          fs.setPermission( dest, CACHED_FILE_PERMISSION );
          for ( FileObject fileObject : source.getChildren() ) {
            copyFile( fileObject, fs, new Path( dest, fileObject.getName().getBaseName() ), overwrite );
          }
        } else {
          try ( FSDataOutputStream fsDataOutputStream = fs.create( dest, overwrite ) ) {
            IOUtils.copy( source.getContent().getInputStream(), fsDataOutputStream );
            fs.setPermission( dest, CACHED_FILE_PERMISSION );
          }
        }
      }

      public String getClusterPathSeparator() {
        return System.getProperty( "hadoop.cluster.path.separator", "," );
      }
    } );
  }

  @Override public void configureConnectionInformation( String namenodeHost, String namenodePort, String jobtrackerHost,
                                                        String jobtrackerPort, org.pentaho.hadoop.shim.api.Configuration conf,
                                                        List<String> logMessages ) throws Exception {
    if ( jobtrackerHost == null || jobtrackerHost.trim().length() == 0 ) {
      throw new Exception( "No job tracker host specified!" );
    }

    if ( jobtrackerPort == null || jobtrackerPort.trim().length() == 0 ) {
      jobtrackerPort = getDefaultJobtrackerPort();
      logMessages.add( "No job tracker port specified - using default: " + jobtrackerPort );
    }

    String jobTracker = jobtrackerHost + ":" + jobtrackerPort;
    conf.set( "mapred.job.tracker", jobTracker );
  }
}
