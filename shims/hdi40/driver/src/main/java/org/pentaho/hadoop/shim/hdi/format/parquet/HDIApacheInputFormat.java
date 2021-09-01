/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2021 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.hdi.format.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.pentaho.hadoop.shim.HadoopShim;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.common.format.ReadFileFilter;
import org.pentaho.hadoop.shim.common.format.ReadFilesFilter;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.ParquetConverter;
import org.pentaho.hadoop.shim.common.format.parquet.delegate.apache.PentahoApacheInputFormat;

import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputDirRecursive;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPathFilter;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;

public class HDIApacheInputFormat extends PentahoApacheInputFormat {
  private HadoopShim shim;
  private org.pentaho.hadoop.shim.api.internal.Configuration pentahoConf;

  public HDIApacheInputFormat( NamedCluster namedCluster ) {
    super( namedCluster );
    inClassloader( () -> {
      shim = new HadoopShim();
      if ( namedCluster != null ) {
        pentahoConf = shim.createConfiguration( namedCluster );
      }
    } );
  }

  @Override
  public void setInputFile( String file ) throws Exception {
    inClassloader( () -> {
      Path filePath = new Path( file );
      FileSystem fs = (FileSystem) this.shim.getFileSystem( this.pentahoConf ).getDelegate();
      filePath = new Path( fs.getUri().toString() + filePath.toUri().getPath() );
      filePath = fs.makeQualified( filePath );
      if ( !fs.exists( filePath ) ) {
        throw new NoSuchFileException( file );
      }
      if ( fs.getFileStatus( filePath ).isDirectory() ) { // directory
        setInputPaths( job, filePath );
        setInputDirRecursive( job, true );
      } else { // file
        setInputPaths( job, filePath.getParent() );
        setInputDirRecursive( job, false );
        setInputPathFilter( job, ReadFileFilter.class );
        job.getConfiguration().set( ReadFileFilter.FILTER_DIR, filePath.getParent().toString() );
        job.getConfiguration().set( ReadFileFilter.FILTER_FILE, filePath.toString() );
      }
    } );
  }

  @Override
  public void setInputFiles( String[] files ) throws Exception {
    inClassloader( () -> {

      boolean pathIsDir = false;
      String[] filePaths = new String[files.length];
      int i = 0;
      for ( String file : files ) {
        Path filePath = new Path( file );
        FileSystem fs = (FileSystem) shim.getFileSystem( pentahoConf ).getDelegate();
        filePath = fs.makeQualified( filePath );
        if ( !fs.exists( filePath ) ) {
          throw new NoSuchFileException( file );
        }
        filePaths[i++] = filePath.toUri().toString();
        if ( fs.getFileStatus( filePath ).isDirectory() ) { // directory
          pathIsDir = true;
        }
      }
      if ( pathIsDir ) { // directory
        setInputPaths( job, String.join( ",", filePaths ) );
        setInputDirRecursive( job, true );
        job.getConfiguration().set( ReadFilesFilter.DIRECTORY, "true" );
        job.getConfiguration().set( ReadFilesFilter.FILE, "false" );
      } else { // file
        setInputPaths( job, String.join( ",", filePaths ) );
        setInputDirRecursive( job, false );
        setInputPathFilter( job, ReadFilesFilter.class );
        job.getConfiguration().set( ReadFilesFilter.FILE, "true" );
        job.getConfiguration().set( ReadFilesFilter.DIRECTORY, "false" );
      }
    } );
  }

  @SuppressWarnings( "squid:S1874" )
  @Override
  public List<IParquetInputField> readSchema( String file ) throws Exception {
    return inClassloader( () -> {
      Configuration conf = job.getConfiguration();
      Path filePath = new Path( file );
      FileSystem fs = (FileSystem) this.shim.getFileSystem( this.pentahoConf ).getDelegate();
      filePath = new Path( fs.getUri().toString() + filePath.toUri().getPath() );
      filePath = fs.makeQualified( filePath );
      FileStatus fileStatus = fs.getFileStatus( filePath );
      List<Footer> footers = ParquetFileReader.readFooters( conf, fileStatus, true );
      if ( footers.isEmpty() ) {
        return new ArrayList<>();
      } else {
        ParquetMetadata meta = footers.get( 0 ).getParquetMetadata();
        MessageType schema = meta.getFileMetaData().getSchema();
        return ParquetConverter.buildInputFields( schema );
      }
    } );
  }
}
