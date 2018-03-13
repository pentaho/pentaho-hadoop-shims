/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.hadoop.shim.common.format.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.orc.Reader;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.api.format.IOrcMetaData;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;
import org.pentaho.hadoop.shim.common.ConfigurationProxy;
import org.pentaho.hadoop.shim.common.format.HadoopFormatBase;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.List;

/**
 * Created by tkafalas on 11/7/2017.
 */
public class PentahoOrcInputFormat extends HadoopFormatBase implements IPentahoOrcInputFormat {

  private String fileName;
  private List<? extends IOrcInputField> inputFields;
  private Configuration conf;

  public PentahoOrcInputFormat() throws Exception {
    conf = inClassloader( () -> {
      Configuration conf = new ConfigurationProxy();
      conf.addResource( "hive-site.xml" );
      return conf;
    } );
  }

  @Override
  public List<IPentahoInputSplit> getSplits() throws Exception {
    return null;
  }

  @Override
  public IPentahoRecordReader createRecordReader( IPentahoInputSplit split ) throws Exception {
    if ( fileName == null || inputFields == null ) {
      throw new IllegalStateException( "fileName or inputFields must not be null" );
    }
    conf = new Configuration();
    return inClassloader( () -> {
      return new PentahoOrcRecordReader( fileName, conf, inputFields );
    } );
  }

  @Override
  public List<IOrcInputField> readSchema( ) throws Exception {
    checkNullFileName();
    Reader orcReader = getReader( );
    return readSchema( orcReader );
  }

  protected List<IOrcInputField> readSchema( Reader orcReader ) throws Exception {
    OrcSchemaConverter OrcSchemaConverter = new OrcSchemaConverter();
    List<IOrcInputField> inputFields = OrcSchemaConverter.buildInputFields( readTypeDescription( orcReader ) );
    IOrcMetaData.Reader orcMetaDataReader = new OrcMetaDataReader( orcReader );
    orcMetaDataReader.read( inputFields );
    return inputFields;
  }

  public TypeDescription readTypeDescription( ) throws Exception {
    checkNullFileName();
    Reader orcReader = getReader( );
    return readTypeDescription( orcReader );
  }

  public TypeDescription readTypeDescription( Reader orcReader ) {
    return orcReader.getSchema();
  }

  private Reader getReader( ) throws Exception {
    return inClassloader( () -> {
      checkNullFileName();
      Path filePath;
      FileSystem fs;
      Reader orcReader;
      try {
        filePath = new Path( fileName );
        fs = FileSystem.get( filePath.toUri(), conf );
        if ( !fs.exists( filePath ) ) {
          throw new NoSuchFileException( fileName );
        }

        if ( fs.getFileStatus( filePath ).isDirectory() ) {
          PathFilter pathFilter = new PathFilter() {
            public boolean accept( Path file ) {
              return file.getName().endsWith( ".orc" );
            }
          };

          FileStatus[] fileStatuses = fs.listStatus( filePath, pathFilter );
          if ( fileStatuses.length == 0 ) {
            throw new NoSuchFileException( fileName );
          }

          filePath = fileStatuses[0].getPath();
        }

        orcReader = OrcFile.createReader( filePath,
          OrcFile.readerOptions( conf ).filesystem( fs ) );
      } catch ( IOException e ) {
        throw new RuntimeException( "Unable to read data from file " + fileName, e );
      }
      return orcReader;
    } );
  }

  /**
   * Set schema from user's metadata
   * <p>
   * This schema will be used instead of schema from {@link #fileName} since we allow user to override pentaho
   * filed name
   */
  @Override
  public void setSchema( List<? extends IOrcInputField> inputFields ) throws Exception {
    this.inputFields = inputFields;
  }

  @Override
  public void setInputFile( String fileName ) throws Exception {
    this.fileName = fileName;
  }

  @Override
  public void setSplitSize( long blockSize ) throws Exception {
    //do nothing 
  }

  public void checkNullFileName() {
    if ( fileName == null ) {
      throw new IllegalStateException( "fileName must not be null" );
    }
  }

}
