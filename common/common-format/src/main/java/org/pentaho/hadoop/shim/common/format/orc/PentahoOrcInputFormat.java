/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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
import org.apache.hadoop.fs.Path;
import org.apache.orc.Reader;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.pentaho.hadoop.shim.api.format.IOrcMetaData;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;
import org.pentaho.hadoop.shim.api.format.SchemaDescription;
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
  private String schemaFileName;
  private SchemaDescription schemaDescription;
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
    if ( fileName == null || schemaDescription == null ) {
      throw new IllegalStateException( "fileName or schemaDescription must not be null" );
    }
    conf = new Configuration();
    return new PentahoOrcRecordReader( fileName, conf, schemaDescription );
  }

  @Override
  public SchemaDescription readSchema( ) throws Exception {
    checkNullFileName();
    Reader orcReader = getReader( );
    return readSchema( orcReader );
  }

  protected SchemaDescription readSchema( Reader orcReader ) throws Exception {
    OrcSchemaConverter OrcSchemaConverter = new OrcSchemaConverter();
    SchemaDescription schemaDescription = OrcSchemaConverter.buildSchemaDescription( readTypeDescription( orcReader ) );
    IOrcMetaData.Reader orcMetaDataReader = new OrcMetaDataReader( orcReader );
    orcMetaDataReader.read( schemaDescription );
    return schemaDescription;
  }

  public TypeDescription readTypeDescription( ) {
    checkNullFileName();
    Reader orcReader = getReader( );
    return readTypeDescription( orcReader );
  }

  public TypeDescription readTypeDescription( Reader orcReader ) {
    return orcReader.getSchema();
  }

  private Reader getReader( ) {
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
      orcReader = OrcFile.createReader( filePath,
        OrcFile.readerOptions( conf ).filesystem( fs ) );
    } catch ( IOException e ) {
      throw new RuntimeException( "Unable to read data from file " + fileName, e );
    }
    return orcReader;
  }

  /**
   * Set schema from user's metadata
   * <p>
   * This schema will be used instead of schema from {@link #schemaFileName} since we allow user to override pentaho
   * filed name
   */
  @Override
  public void setSchema( SchemaDescription schema ) throws Exception {
    schemaDescription = schema;
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
