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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.hadoop.shim.api.format.IOrcInputField;
import org.pentaho.hadoop.shim.api.format.IOrcMetaData;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;
import org.pentaho.hadoop.shim.common.format.S3NCredentialUtils;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by tkafalas on 11/7/2017.
 */
public class PentahoOrcRecordReader implements IPentahoOrcInputFormat.IPentahoRecordReader {
  private static final Logger logger = Logger.getLogger( PentahoOrcRecordReader.class );
  private final Configuration conf;
  private final List<? extends IOrcInputField> dialogInputFields;  //Comes from Dialog
  private final List<? extends IOrcInputField> orcInputFields;  //Comes from OrcFile combined with custom metadata
  private VectorizedRowBatch batch;
  private RecordReader recordReader;
  private int currentBatchRow;
  private TypeDescription typeDescription;
  private Map<String, Integer> schemaToOrcSubcripts;
  private Path filePath;
  private FileSystem fs;
  private OrcConverter orcConverter = new OrcConverter();

  public PentahoOrcRecordReader( String fileName, Configuration conf, List<? extends IOrcInputField> dialogInputFields ) {
    this.conf = conf;
    this.dialogInputFields = dialogInputFields;

    Reader reader = null;
    try {
      S3NCredentialUtils.applyS3CredentialsToHadoopConfigurationIfNecessary( fileName, conf );
      filePath = new Path( S3NCredentialUtils.scrubFilePathIfNecessary( fileName ) );
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

      reader = OrcFile.createReader( filePath,
        OrcFile.readerOptions( conf ).filesystem( fs ) );
    } catch ( IOException e ) {
      throw new IllegalArgumentException( "Unable to read data from file " + fileName, e );
    }
    try {
      recordReader = reader.rows();
    } catch ( IOException e ) {
      throw new IllegalArgumentException( "Unable to get record reader for file " + fileName, e );
    }
    typeDescription = reader.getSchema();
    OrcSchemaConverter orcSchemaConverter = new OrcSchemaConverter();
    orcInputFields = orcSchemaConverter.buildInputFields( typeDescription );
    IOrcMetaData.Reader orcMetaDataReader = new OrcMetaDataReader( reader );
    orcMetaDataReader.read( orcInputFields );
    batch = typeDescription.createRowBatch();

    //Create a map of orc fields to meta columns
    Map<String, Integer> orcColumnNumberMap = new HashMap<String, Integer>();
    int orcFieldNumber = 0;
    for ( String orcFieldName : typeDescription.getFieldNames() ) {
      orcColumnNumberMap.put( orcFieldName, orcFieldNumber++ );
    }

    //Create a map of input fields to Orc Column numbers
    schemaToOrcSubcripts = new HashMap<String, Integer>();
    for ( IOrcInputField inputField : dialogInputFields ) {
      if ( inputField != null ) {
        Integer colNumber = orcColumnNumberMap.get( inputField.getFormatFieldName() );
        if ( colNumber == null ) {
          throw new IllegalArgumentException(
            "Column " + inputField.getFormatFieldName() + " does not exist in the ORC file.  Please use the getFields button" );
        } else {
          schemaToOrcSubcripts.put( inputField.getPentahoFieldName(), colNumber );
        }
      }
    }

    try {
      setNextBatch();
    } catch ( IOException e ) {
      throw new IllegalArgumentException( "No rows to read in " + fileName, e );
    }
  }


  private boolean setNextBatch() throws IOException {
    currentBatchRow = 0;
    return recordReader.nextBatch( batch );
  }

  @Override public void close() throws IOException {
    recordReader.close();
  }

  @Override public Iterator<RowMetaAndData> iterator() {
    return new Iterator<RowMetaAndData>() {

      @Override public boolean hasNext() {
        if ( currentBatchRow < batch.size ) {
          return true;
        }
        try {
          return setNextBatch();
        } catch ( IOException e ) {
          e.printStackTrace();
          return false;
        }
      }

      @Override public RowMetaAndData next() {
        RowMetaAndData rowMeta =
          orcConverter.convertFromOrc( batch, currentBatchRow, dialogInputFields, typeDescription, schemaToOrcSubcripts, orcInputFields );
        currentBatchRow++;
        return rowMeta;
      }
    };
  }

}
